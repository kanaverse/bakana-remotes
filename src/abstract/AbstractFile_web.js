export class AbstractFile {
    #buffer;

    constructor(obj) {
        if (obj instanceof File) {
            let reader = new FileReaderSync();
            this.#buffer = reader.readAsArrayBuffer(obj);
        } else if (obj instanceof ArrayBuffer) {
            this.#buffer = obj; // assumed to already be an ArrayBuffer.
        } else {
            throw "unknown type '" + typeof(obj) + "' for LoadedFile constructor";
        }
    }

    buffer() {
        return this.#buffer;
    }
};

export function dumpToTemporary(buffer) {
    return buffer;
}

