import * as fs from "fs";
import * as os from "os";

export class AbstractFile {
    constructor(obj) {
        this.path = obj;
    }

    buffer() {
        let f = fs.readFileSync(this.path);
        return f.buffer.slice(f.byteOffset, f.byteOffset + f.byteLength);
    }
};

export function dumpToTemporary(buffer) {
    // TODO: remove this and other functions once bakana learns
    // to accept buffers in the embeddedSaver for Node.js.
    let stuff = fs.mkdtempSync(os.tmpdir() + "/bakana-remotes-");
    let path = stuff + "/id.txt";
    fs.writeFileSync(path, buffer);
    return path;
}
