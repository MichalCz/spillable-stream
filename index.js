const {Duplex, PassThrough, 
    // eslint-disable-next-line no-unused-vars
    Transform
} = require("stream");
const {join} = require("path");
const {tmpdir} = require("os");
const {promisify} = require("util");
const mkpath = promisify(require("mkpath"));
const {createWriteStream, createReadStream, rmdir, unlink} = require("fs");
const {DataStream} = require("scramjet");
const fini = require("infinite-sequence-generator");
const assert = require("assert");

const [rmdirp, unlinkp] = [
    promisify(rmdir),
    promisify(unlink)
];
const _seq = fini();
const nextPath = (path) => join(path, `.node-spill-${process.pid}-${_seq.next().value}.chunks`);

class SpillStream extends Duplex {
    /**
     * 
     * @param {Object} options Stream options
     * @param {boolean} [options.objectMode] Object mode toggle
     * @param {string} [options.workdir=os.tmpdir()] Working directory to create files
     * @param {number} [options.chunkSize=1048576] Standard chunk size
     * @param {number} [options.maxDrift=options.chunkSize] Maximum drift between read and write before using disk
     * @param {Function|AsyncFunction|Transform} [serialize] [NYI] unimplemented serialization stream handler
     * @param {Function|AsyncFunction|Transform} [deserialize] [NYI] unimplemented deserialization stream handler
     * @param  {...any} args 
     */
    constructor(options, ...args) {
        assert.ok(
            options.objectMode,
            "Object mode not supported yet"
        );
        assert.ok(
            options.readableObjectMode !== options.objectMode || options.writableObjectMode !== options.objectMode,
            "Cannot set different readable and writable object modes, use objectMode"
        );

        super(options, ...args);
        
        // memorize options
        this._workdir = options.workdir || tmpdir();
        this._chunkSize = options._chunkSize || 1<<20;
        this._maxDrift = options.maxDrift || this._chunkSize;
        this._options = options;

        // initialize chunk list values
        this._finished = false;

        this._files = null;
        this._cleanups = null;
        this._path = "";
        const freed = this._freed = [];
        
        this._reset();
        
        // setup chunk filenames generator
        this._paths = DataStream
            .from(function* () {
                const names = fini("", "01234567890abcdef");
                while (true) {
                    if (freed.length) {
                        yield* freed.splice(0);
                    } else {
                        yield names.next().value;
                    }
                }
            })
            .map(
                async name => {
                    const parts = [this._path, name.match(/.{1,2}/g)];
                    const filename = `${join(parts)}.chunk`;
                    const dirname = join(parts.slice(0, parts.length - 1));
            
                    await mkpath(dirname);
                    
                    for (let i = 1; i < parts.length - 1; i++) this._cleanups.add(join(...parts.slice(0, i)));
                    for (let i = parts.length - 2; i >= 0; i--) this._files.add(filename);

                    return filename;
                }
            )
            .toGenerator();

        
        /**
         * Allowable drift buffer.
         * 
         * Drift buffer has a highWaterMark set to maxDrift. `write` will return true as soon as we sway over the limit
         * which later causes to start spilling to disk.
         * 
         * @internal
         * @type {PassThrough<any>}
         */
        this._drift = null;

        /**
         * A queue of chunk filenames written to disk
         * @type {string[]}
         */
        this._bufferNames = [];

        /**
         * Writable stream to disk
         * @internal
         * @type {WritableStream<String>}
         */
        this._writeBuffer = null;

        /**
         * Readble stream from disk
         * @internal
         * @type {ReadableStream<String>}
         */
        this._readBuffer = null;

        /**
         * Filename currently read.
         * @internal
         * @type {string}
         */
        this._readBufferFile = "";
        
        // setup drift buffer
        this._resetDrift();

        // event tracking utilty fields
        this._readablePromise = null;
        this._readableListener = null;
        this._drainPromise = null;
        this._drainListener = null;

        /**
         * Counts the number of bytes written
         * 
         * @type {number}
         */
        this._wrote = 0;
    }

    get reading() {
        if (this._readBuffer === null) {
            return this._drift;
        } else {
            return this._readBuffer;
        }
    }
    
    get writing() {
        if (this._writeBuffer) {
            return this._writeBuffer;
        } else {
            return this._drift;
        }
    }

    _pushCleanup(file) {
        this._cleanups.push(file);
    }
    
    async _write(chunk, encoding, callback) {
        if (chunk === null) this._finished = true;
        else if (this._writeBuffer && this._account(chunk)) await this._rotateWrite();
        
        await this._whenWrote(chunk, encoding);
        callback();
    }

    async _read(size) {
        await this._whenReadable;
        let chunk;
        let zise = size;
        while (null !== (chunk = this.readable.read(zise))) {
            zise -= chunk && chunk.length || 1;
            this.push(chunk);
        }
    }

    async _destroy(err, callback) {
        try {
            await this._cleanup();
            callback(err);
        } catch (e) {
            callback(e);
        }
    }

    async _reset() {
        this._path = nextPath(this._workdir);
        this._files = new Set();
        this._cleanups = new Set();
    }

    async _cleanup() {
        const files = this._files;
        const cleanups = this._cleanups;

        this._reset();
        
        if (files && files.size) await Promise.all(Array.from(files).map(unlinkp));
        if (cleanups && cleanups.size){
            const levels = Array.from(cleanups).reduce((acc, todelete) => {
                // sibling paths have same length here!
                (acc[todelete.length] = acc[todelete.length] || []).push(todelete);
            }, []).filter(x => x);

            for (let level of levels) {
                await Promise.all(level.map(rmdirp));
            }
        }
    }

    _account(chunk) {
        return (this._wrote += chunk.length) > this._chunkSize;
    }

    _rotateRead() {
        if (this._bufferNames.length) {
            // free up the read filename
            if (this._readBufferFile) 
                unlinkp(this._readBufferFile)
                    .then(() => this._freed.push(this._readBufferFile))
                    .catch(err => this._emitWarning(err, "CannotRemoveFileWarning", "ERMF001"));
                
            this._readBufferFile = this._bufferNames.shift();
            this._readBuffer = createReadStream(this._readBufferFile);
            if (!this._bufferNames.length) {
                this._resetDrift();
            }
        } else if (!this._finished) {
            this._readBufferFile = "";
            this._readBuffer = this._drift;
        } else {
            this._readBuffer = null;
            this.push(null);
        }

        if (this._readBuffer) this._readBuffer.on("end", this._rotateRead);
    }

    _emitWarning(...args) {
        if (this.listenerCount("warning")) this.emit("warning", ...args);
        else process.emitWarning(...args);
    }

    _resetDrift() {
        if (this._writeBuffer) {
            this._writeBuffer.end();
            this._writeBuffer.removeListener("drain", this._drain);
            this._writeBuffer = null;
        }
        this._bufferNames.push(new PassThrough({...this._options, highWaterMark: this._maxDrift}));
    }

    async _rotateWrite() {
        const nextFile = await this._paths.get().value;
        if (this._writeBuffer) {
            this._writeBuffer.end();
            this._writeBuffer.removeListener("drain", this._drain);
        }
        this._writeBuffer = createWriteStream(nextFile);
        this._writeBuffer.on("drain", this._drain);
        this._files.push(nextFile);
        this._bufferNames.push(nextFile);
    }

    get _drain() {
        // getter for a bound function
        const value = () => {
            if (!this._drainListener) return;

            const listener = this._drainListener;
            this._drainListener = this._drainPromise = null;
            
            listener();
        };

        Object.defineProperty(this, "_drain", {value});
        return value;
    }

    get _readable() {
        // getter for a bound function
        const value = () => {
            if (!this._readableListener) return;

            const listener = this._readableListener;
            this._readableListener = this._readablePromise = null;
            
            listener();
        };

        Object.defineProperty(this, "_readable", {value});
        return value;
    }

    async _whenWrote(chunk, encoding) {
        if (!this.writing.write(chunk, encoding)) {
            if (this._writeBuffer) {
                await this._whenDrained;
            } else {
                this._rotateWrite();
            }
        }
    }

    get _whenReadable() {
        return this.reading.readable || (
            this._readablePromise || (this._readablePromise = new Promise(res => this._readableListener = res))
        );
    }

    get _whenDrained() {
        return this._drainPromise || (this._drainPromise = new Promise(res => this._drainListener = res));
    }
}

module.exports = SpillStream;