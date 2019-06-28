import { PassThrough, TransformOptions } from "stream";

export class SpillStream extends PassThrough {
    constructor(options?: { 
        workdir?: string,
        chunkSize? : number,
        maxDrift? : number
    } & TransformOptions);
}
