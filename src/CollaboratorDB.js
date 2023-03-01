import * as scran from "scran.js";
import * as bioc from "bioconductor";
import * as bakana from "bakana";
import * as adb from "artifactdb";
import * as utils from "./utils.js";

const baseUrl = "https://collaboratordb.aaron-lun.workers.dev";

var getFun = null;
var downloadFun = null;

class CollaboratordbNavigator {
    #project;
    #version;

    constructor(project, version) {
        this.#project = project;
        this.#version = version;
    }

    async file(path) {
        let id = adb.packId(this.#project, path, this.#version);
        return await adb.getFile(baseUrl, id, { getFun: getFun, downloadFun: downloadFun });
    }

    async metadata(path) {
        let id = adb.packId(this.#project, path, this.#version);
        return await adb.getFileMetadata(baseUrl, id, { getFun: getFun });
    }
};

/**
 * Dataset derived from a SummarizedExperiment in [CollaboratorDB](https://github.com/CollaboratorDB).
 * This extends the [AbstractArtifactdbDataset](https://kanaverse.github.io/bakana/AbstractArtifactdbDataset.html) class.
 */
export class CollaboratordbDataset extends bakana.AbstractArtifactdbDataset {
    /** 
     * @param {?function} fun - Function that accepts a URL string and downloads the resource,
     * returning a Uint8Array of the file contents.
     * Alternatively, on Node.js, the function may return a string containing a file path to the downloaded resource.
     * 
     * Alternatively `null`, to reset the function to its default value.
     * See [`getFile`](https://artifactdb.github.io/artifactdb.js/global.html#getFile) for details.
     * @return {?function} Previous setting of the download function.
     */
    static setDownloadFun(fun) {
        let previous = downloadFun;
        downloadFun = fun;
        return previous;
    }

    /** 
     * @param {?function} fun - Function that accepts a URL string and performs a GET to return a Response object,
     * see [`getFileMetadata`](https://artifactdb.github.io/artifactdb.js/global.html#getFileMetadata) for details.
     * 
     * Alternatively `null`, to reset the function to its default value.
     * @return {?function} Previous setting of the GET function.
     */
    static setGetFun(fun) {
        let previous = getFun;
        getFun = fun;
        return previous;
    }

    /****************************************
     ****************************************/

    #id;
    #unpacked;

    /**
     * @param {string} id - Identifier of a SummarizedExperiment in CollaboratorDB.
     * @param {object} [options={}] - Optional parameters, including those passed to the `options=` argument of the 
     * [ArtifactdbSummarizedExperimentDatasetBase](https://kanaverse.github.io/bakana/ArtifactdbSummarizedExperimentDatasetBase.html) constructor.
     */
    constructor(id, options = {}) {
        let unpacked = adb.unpackId(id);
        super(unpacked.path, new CollaboratordbNavigator(unpacked.project, unpacked.version), options);
        this.#id = id;
        this.#unpacked = unpacked;
        return;
    }

    /**
     * @return {string} Format of this dataset class.
     * @static
     */
    static format() {
        return "CollaboratorDB";
    }

    /**
     * @return {object} Object containing the abbreviated details of this dataset.
     */
    abbreviate() {
        return { 
            "id": this.#id, 
            "options": this.options()
        };
    }

    /**
     * @return {object} Object describing this dataset, containing:
     *
     * - `files`: Array of objects representing the files used in this dataset.
     *   Each object corresponds to a single file and contains:
     *   - `type`: a string denoting the type.
     *   - `file`: a {@linkplain SimpleFile} object representing the file contents.
     * - `options`: An object containing additional options to saved.
     */
    serialize() {
        const enc = new TextEncoder;
        let buffer = enc.encode(this.#id);

        // Storing it as a string in the buffer.
        let output = {
            type: "id",
            file: new bakana.SimpleFile(buffer, { name: "id" })
        };

        return {
            files: [ output ],
            options: this.options()
        }
    }

    /**
     * @param {Array} files - Array of objects like that produced by {@linkcode CollaboratordbDataset#serialize serialize}.
     * @param {object} options - Object containing additional options to be passed to the constructor.
     * @return {CollaboratordbDataset} A new instance of this class.
     * @static
     */
    static unserialize(files, options) {
        let args = {};

        // This should contain 'id'.
        for (const x of files) {
            const dec = new TextDecoder;
            args[x.type] = dec.decode(x.file.buffer());
        }

        if (!("id" in args)) {
            throw new Error("expected a file of type 'id' when unserializing CollaboratorDB dataset"); 
        }
        return new CollaboratordbDataset(args.id, options);
    }
}
