const path = require('path');

const readFile = (require('util')).promisify(require('fs').readFile);

const ThumbnailCache = require('./index');

const ourCache = new ThumbnailCache(
    'cache',
    reqPath => readFile(path.join('testdata', reqPath)),
    { max: 256 * 1024, preserve: 2 * 1024 * 1024 * 1024 },
    console
);

const app = new (require('koa'))();

app.use(async ctx => {
    const dim = parseInt(ctx.query.dim);
    if (isNaN(dim) || dim > 4096 || dim < 4) {
        ctx.throw(400, 'invalid dimension');
    }
    const imgBuffer = await ourCache.getThumbnail(ctx.path, dim);
    ctx.type = 'image/jpeg';
    ctx.body = imgBuffer;
});

app.listen(8000);
console.log('Test server launched @ 8000');
