import * as bakana from "bakana";
import * as utils from "./utils.js";

class SewerRatNavigator {
    #url;
    #prefix;
    #download;
    #check;

    constructor(prefix, url, download, check) {
        this.#prefix = prefix;
        this.#url = url;
        this.#download = download;
        this.#check = check;
    }

    async get(path, asBuffer) {
        return this.#download(this.#url + "/retrieve/file?path=" + encodeURIComponent(this.#prefix + "/" + path));
    }

    async exists(path) {
        return this.#check(this.#url + "/retrieve/file?path=" + encodeURIComponent(this.#prefix + "/" + path));
    }

    clean(localPath) {}
}

async function defaultCheck(u) {
    let resp = await fetch(u, { method: "HEAD" });
    if (resp.ok) {
        return true;
    } else if (resp.status == 404) {
        return false;
    } else {
        throw new Error("failed to check existence of '" + u + "' (" + String(resp.status) + ")");
    }
}

/**
 * Dataset represented by a SummarizedExperiment in a [**SewerRat**](https://github.com/ArtifactDB/SewerRat)-registered directory.
 * This extends the [AbstractAlabasterDataset](https://kanaverse.github.io/bakana/AbstractAlabasterDataset.html) class.
 */
export class SewerRatDataset extends bakana.AbstractAlabasterDataset {
    #id;

    static #downloadFun = utils.defaultDownload;

    /**
     * @param {function} fun - A (possibly `async`) function that accepts a URL and returns a Uint8Array of that URL's contents.
     * @return {function} Previous setting of the download function.
     */
    static setDownloadFun(fun) {
        let previous = SewerRatDataset.#downloadFun;
        SewerRatDataset.#downloadFun = fun;
        return previous;
    }

    static #checkFun = defaultCheck;

    /**
     * @param {function} fun - A (possibly `async`) function that accepts a URL, performs a HEAD request and returns a Response object.
     * @return {function} Previous setting of the HEAD function.
     */
    static setCheckFun(fun) {
        let previous = SewerRatDataset.#checkFun;
        SewerRatDataset.#checkFun = fun;
        return previous;
    }

    /**
     * @param {string} path - Absolute path to a SummarizedExperiment on the **SewerRat**'s filesystem.
     * @param {string} url - URL to the **SewerRat** REST API. 
     */
    constructor(path, url) {
        super(new SewerRatNavigator(path, url, SewerRatDataset.#downloadFun, SewerRatDataset.#checkFun));
        this.#id = { path, url };
    }

    /**
     * @return {string} Format of this dataset class.
     * @static
     */
    static format() {
        return "SewerRat";
    }

    /**
     * @return {object} Object containing the abbreviated details of this dataset.
     */
    abbreviate() {
        return { id: { ...(this.#id) }, options: this.options() };
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
        const buffer = enc.encode(JSON.stringify(this.#id));

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
    static async unserialize(files, options) {
        let args = {};

        // This should contain 'id'.
        for (const x of files) {
            const dec = new TextDecoder;
            args[x.type] = dec.decode(x.file.buffer());
        }

        if (!("id" in args)) {
            throw new Error("expected a file of type 'id' when unserializing SewerRat dataset"); 
        }
        const id = JSON.parse(args.id);

        let output = new SewerRatDataset(id.path, id.url);
        output.setOptions(options);
        return output;
    }
}

/**
 * Result represented as a SummarizedExperiment in the [**gypsum**](https://github.com/ArtifactDB/gypsum-worker) store.
 * This extends the [AbstractAlabasterResult](https://kanaverse.github.io/bakana/AbstractAlabasterResult.html) class.
 */
export class SewerRatResult extends bakana.AbstractAlabasterResult {
    static #downloadFun = utils.defaultDownload;

    /**
     * @param {function} fun - A (possibly `async`) function that accepts a URL and returns a Uint8Array of that URL's contents.
     * @return {function} Previous setting of the download function.
     */
    static setDownloadFun(fun) {
        let previous = SewerRatResult.#downloadFun;
        SewerRatResult.#downloadFun = fun;
        return previous;
    }

    static #checkFun = defaultCheck;

    /**
     * @param {function} fun - A (possibly `async`) function that accepts a URL and returns a boolean indicating whether that URL can be accessed without a 404.
     * @return {function} Previous setting of the HEAD function.
     */
    static setCheckFun(fun) {
        let previous = SewerRatResult.#checkFun;
        SewerRatResult.#checkFun = fun;
        return previous;
    }

    /**
     * @param {string} path - Absolute path to a SummarizedExperiment on the **SewerRat**'s filesystem.
     * @param {string} url - URL to the **SewerRat** REST API. 
     */
    constructor(path, url) {
        super(new SewerRatNavigator(path, url, SewerRatResult.#downloadFun, SewerRatResult.#checkFun));
    }
}
