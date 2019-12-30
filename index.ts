import fs = require('fs');
import path = require('path');
import sharp = require('sharp');
import util = require('util');
import statvfsRaw = require('statvfs');

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);
const unlink = util.promisify(fs.unlink);
const writeFile = util.promisify(fs.writeFile);

const statvfs = util.promisify(statvfsRaw);

export = class ThumbnailCache {

    private readonly _path: string;
    private readonly _sizeMax: number;
    private readonly _sizePreserve: number;
    private readonly _console: Console;
    private _writeQueue: { writes: Map<string, Buffer>, snapshot: Map<string, number> };

    /**
     * @param cacheDir path for storing images
     * @param _source source for getting full-sized images asynchronously (via local filesystem, HTTP, etc.)
     * @param size.max the maximum cache size, in bytes; cache won't grow bigger than this, no matter how large the disk is.
     * @param size.preserve always make sure there are still so many available bytes left on disk.
     * @param opt_console if given, will be used for writing debugging logs
     */
    constructor(cacheDir: string, private _source: (path: string) => Promise<Buffer>, size: { max: number, preserve: number }, opt_console: Console) {
        this._path = path.resolve(cacheDir);
        this._sizeMax = (size ? size.max : 0) || 512 * 1024 * 1024;
        this._sizePreserve = (size ? size.preserve : 0) || 256 * 1024 * 1024;
        this._console = opt_console || null;
        this._writeQueue = null;
        
        try {
            fs.mkdirSync(this._path);
        } catch (e) {
            if (e.code !== 'EEXIST') {
                throw e;
            }
        }
    }

    async getThumbnail(url: string, maxDimension: number): Promise<Buffer> {
        const key = url.replace(/[^A-Za-z0-9_\-\.]/g, r => '(' + r.charCodeAt(0).toString(36) + ')') + '@' + maxDimension;
        try {
            const fullPath = path.resolve(this._path, key);
            const cacheContent = await readFile(fullPath);
            const now = new Date();
            if (this._writeQueue) {
                this._writeQueue.snapshot.delete(key);
                this._writeQueue.snapshot.set(key, cacheContent.byteLength); // Put it at the end.
            }
            fs.utimes(fullPath, now, now, err => { // Not waiting for it before return.
                if (err) {
                    this._error(`Error when setting mtime of ${key}`, err);
                }
            });
            this._log(`Cache hit for ${key}.`);
            return cacheContent;
        } catch (e) {
            if (e.code !== 'ENOENT') {
                this._error(`Error when attempting to read ${key}.`, e);
            }
        }
        this._log(`Cache miss for ${key}.`);
        // Either way, create the thumbnail in-memory.
        const rawBuffer = await this._source(url);
        const resizedBuffer = await sharp(rawBuffer).resize(maxDimension, maxDimension, { fit: 'inside' }).toBuffer();
        this._saveToCache(key, resizedBuffer);
        return resizedBuffer;
    }

    private async _getFreeSpace(): Promise<number> {
        const stats = await statvfs(this._path);
        return stats.bsize * stats.bavail;
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
                .map(({ mtime, size }, index) => ({ key: files[index], mtime, size }))
                .sort((a, b) => a.mtime.getTime() - b.mtime.getTime())
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
        // Get the available space on disk.
        const availableSpace = await this._getFreeSpace();
        const targetSizeLimitation = Math.max(Math.min(this._sizeMax, availableSpace - this._sizePreserve + totalSize), 1024);
        this._log(`Free size in disk: ${availableSpace} bytes; max cache size ${targetSizeLimitation}.`);
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