import fs = require('fs/promises');
import path = require('path');
import sharp = require('sharp');

const { readFile, readdir, stat, unlink, writeFile } = fs;

type BufferWithTimestamp = Buffer & { fetchedTime: Date };
const withTimestamp = (buffer: Buffer, fetchedTime: Date): BufferWithTimestamp => {
    (buffer as BufferWithTimestamp).fetchedTime = fetchedTime;
    return buffer as BufferWithTimestamp;
};

export default class ThumbnailCache {

    private readonly _path: string;
    private readonly _sizeMax: number;
    private readonly _console: Console;
    private readonly _jpegOptions: sharp.JpegOptions;
    private readonly _fallbackToOriginal: boolean;
    private _writeQueue: { writes: Map<string, Buffer>, snapshot: Map<string, number> };

    /**
     * @param cacheDir path for storing images
     * @param _source source for getting full-sized images asynchronously (via local filesystem, HTTP, etc.)
     * @param size.max the maximum cache size, in bytes; cache won't grow bigger than this, no matter how large the disk is.
     * @param options.console if given, will be used for writing debugging logs
     * @param options.jpegOptions if given, will be used as JPEG output options for sharp
     * @param options.fallbackToOriginal if true, will return the original image in case there's an error in resizing
     */
    constructor(cacheDir: string, private _source: (path: string) => Promise<Buffer>, size: { max: number }, options?: { console?: Console, jpegOptions?: sharp.JpegOptions, fallbackToOriginal?: boolean }) {
        this._path = path.resolve(cacheDir);
        this._sizeMax = (size ? size.max : 0) || 512 * 1024 * 1024;
        this._console = options ? options.console || null : null;
        this._jpegOptions = { force: true, quality: 80, ...(options ? options.jpegOptions : null) };
        this._fallbackToOriginal = !!options.fallbackToOriginal;
        this._writeQueue = null;

        fs.mkdir(this._path).catch(e => {
            if (e.code !== 'EEXIST') {
                throw e;
            }
        });
    }

    /**
     * @param url the URL to fetch source image from. will be handled by the `_source` given.
     * @param maxDimension the longer side of the image. return the original image w/o resizing when <= 0
     */
    async getThumbnail(url: string, maxDimension: number): Promise<BufferWithTimestamp> {
        const key = url.replace(/[^A-Za-z0-9_\-\.]/g, r => '(' + r.charCodeAt(0).toString(36) + ')') + '@' + maxDimension;
        try {
            const fullPath = path.resolve(this._path, key);
            const [cacheContent, cacheStat] = await Promise.all([readFile(fullPath), stat(fullPath)]);
            if (this._writeQueue) {
                this._writeQueue.snapshot.delete(key);
                this._writeQueue.snapshot.set(key, cacheContent.byteLength); // Put it at the end.
            }
            const now = new Date();
            fs.utimes(fullPath, now, cacheStat.mtime).catch(err => {
                if (err) {
                    this._error(`Error when setting mtime of ${key}`, err);
                }
            });
            this._log(`Cache hit for ${key}.`);
            return withTimestamp(cacheContent, cacheStat.mtime);
        } catch (e) {
            if (e.code !== 'ENOENT') {
                this._error(`Error when attempting to read ${key}.`, e);
            }
        }
        this._log(`Cache miss for ${key}.`);
        // Either way, create the thumbnail in-memory.
        const rawBuffer = await this._source(url);
        const resizedBuffer = (maxDimension > 0) ? await this.resize(key, rawBuffer, maxDimension) : rawBuffer;
        this._saveToCache(key, resizedBuffer);
        return withTimestamp(resizedBuffer, new Date());
    }

    private async resize(key: string, rawBuffer: Buffer, maxDimension: number): Promise<Buffer> {
        try {
            return await sharp(rawBuffer).resize(maxDimension, maxDimension, { fit: 'inside' }).jpeg(this._jpegOptions).toBuffer();
        } catch (e) {
            if (this._fallbackToOriginal) {
                this._error(`Failed to resize ${key}`, e);
                return rawBuffer;
            } else {
                throw e;
            }
        }
    }

    private _log(...args: any[]): void {
        if (this._console) { this._console.log(...args); }
    }

    private _error(...args: any[]): void {
        if (this._console) { this._console.error(...args); }
    }

    private _saveToCache(key: string, buffer: Buffer): void {
        if (this._writeQueue) {
            this._writeQueue.writes.set(key, buffer);
        } else {
            this._writeQueue = {
                writes: new Map([[key, buffer]]),
                snapshot: new Map() // From key to size, strictly in the ascending order of mtime.
            };
            this._processWriteQueue().catch(e => {
                this._error(e);
            });
        }
    }

    private async _getCacheSnapshot(): Promise<Map<string, number>> {
        const files = await readdir(this._path);
        return new Map(
            (await Promise.all(files.map(f => stat(path.resolve(this._path, f)))))
                .map(({ atime, size }, index) => ({ key: files[index], atime, size }))
                .sort((a, b) => a.atime.getTime() - b.atime.getTime())
                .map(({ key, size }) => [key, size])); // All files, sorted from old to new.
    }

    private async _processWriteQueue(): Promise<void> {
        // And the snapshots.
        const fullSnapshot = await this._getCacheSnapshot();
        // Item might be be added to temporarySnapshot, by the reads during getCacheSnapshot execution.
        const temporarySnapshot = this._writeQueue.snapshot;
        temporarySnapshot.forEach((v, k) => {
            fullSnapshot.delete(k);
            fullSnapshot.set(k, v);
        });
        let totalSize = Array.from(fullSnapshot.values()).reduce((a, b) => a + b, 0);
        this._log(`Snapshot ready (total size: ${totalSize} bytes).`, fullSnapshot);
        // Now we have a full snapshot (which may still change over time).
        this._writeQueue.snapshot = fullSnapshot;
        const targetSizeLimitation = Math.max(this._sizeMax, 1024);
        const writesIt = this._writeQueue.writes.entries();
        for (let cur = writesIt.next(); !cur.done; cur = writesIt.next()) {
            const [key, buffer] = cur.value;
            const size = buffer.byteLength;
            // If single file is too large, skip.
            if (size >= targetSizeLimitation) {
                this._log(`Skipping ${key} as it alone is too large.`);
                continue;
            }
            // Purge some old ones.
            while (totalSize + size > targetSizeLimitation) {
                const [victimKey, victimSize] = fullSnapshot.entries().next().value;
                try {
                    this._log(`Purging ${victimKey} (${victimSize}) from the cache.`);
                    await unlink(path.resolve(this._path, victimKey));
                    fullSnapshot.delete(victimKey);
                } catch (e) {
                    if (e.code !== 'ENOENT') { throw e; }
                }
                totalSize -= victimSize;
            }
            // Add the new one.
            await writeFile(path.resolve(this._path, key), buffer);
            fullSnapshot.set(key, size);
            totalSize += size;
            this._log(`Saved ${key} to the cache.`);
        }
        this._log(`Write queue cleared.`);
        this._writeQueue = null;
    }

}