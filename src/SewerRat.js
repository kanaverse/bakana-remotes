import * as scran from "scran.js";
import * as bioc from "bioconductor";
import * as bakana from "bakana";
import * as bt from "bakana-takane";
import * as utils from "./utils.js";

var getFunDataset = null;
var getFunResult = null;
var listFunDataset = null;
var listFunResult = null;

async function defaultGetFun(u) {
    const res = await fetch(u);
    if (res.ok) {
        const err = Error("failed to fetch '" + u + "'");
        err.status_code = res.status_code;
        return err;
    }
    return new Uint8Array(await res.arrayBuffer());
}

async function defaultListFun(u) {
    const res = await fetch(u);
    if (res.ok) {
        const err = Error("failed to fetch '" + u + "'");
        err.status_code = res.status_code;
        return err;
    }
    return res.json();
}

function createSewerRatFunctions(url, get, list) {
    const getter = p => get(url + "/retrieve/file?path=" + encodeURIComponent(p));
    const lister = p => list(url + "/list?path=" + encodeURIComponent(p));
    return { getter, lister };
}

/**
 * Dataset represented by a SummarizedExperiment in a [**SewerRat**](https://github.com/ArtifactDB/SewerRat)-registered directory.
 * This extends the [AbstractDataset](https://kanaverse.github.io/bakana-takane/AbstractDataset.html) class.
 */
export class SewerRatDataset extends bt.AbstractDataset {
    #id;

    /**
     * @param {function} fun - A (possibly `async`) function that accepts a URL and returns a Uint8Array of that URL's contents.
     * Alternatively `null`, to reset the function to its default value based on `fetch`.
     * @return {?function} Previous setting of the getter function.
     */
    static setGetFun(fun) {
        let previous = getFunDataset;
        getFunDataset = fun;
        return previous;
    }

    /**
     * @param {function} fun - A (possibly `async`) function that accepts a URL and returns a JSON object.
     * Alternatively `null`, to reset the function to its default value based on `fetch`.
     * @return {?function} Previous setting of the listing function.
     */
    static setListFun(fun) {
        let previous = listFunDataset;
        listFunDataset = fun;
        return previous;
    }

    /**
     * @param {string} path - Absolute path to a SummarizedExperiment on the **SewerRat**'s filesystem.
     * @param {string} url - URL to the **SewerRat** REST API. 
     */
    constructor(path, url) {
        let get = getFunDataset;
        if (get === null) {
            get = defaultGetFun;
        }
        let list = listFunDataset;
        if (list === null) {
            list = defaultListFun;
        }
        const { getter, lister } = createSewerRatFunctions(url, get, list);
        super(path, getter, lister);
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
 * This extends the [AbstractResult](https://kanaverse.github.io/bakana-takane/AbstractResult.html) class.
 */
export class SewerRatResult extends bt.AbstractResult {
    /**
     * @param {function} get - A (possibly `async`) function that accepts a URL and returns a Uint8Array of that URL's contents.
     * Alternatively `null`, to reset the function to its default value based on `fetch`.
     * @return {?function} Previous setting of the GET function.
     */
    static setGetFun(fun) {
        let previous = getFunResult;
        getFunResult = fun;
        return previous;
    }

    /**
     * @param {function} fun - A (possibly `async`) function that accepts a URL and returns a JSON object.
     * Alternatively `null`, to reset the function to its default value based on `fetch`.
     * @return {?function} Previous setting of the listing function.
     */
    static setListFun(fun) {
        let previous = listFunDataset;
        listFunDataset = fun;
        return previous;
    }

    /**
     * @param {string} path - Absolute path to a SummarizedExperiment on the **SewerRat**'s filesystem.
     * @param {string} url - URL to the **SewerRat** REST API. 
     */
    constructor(path, url) {
        let get = getFunResult;
        if (get === null) {
            get = defaultGetFun;
        }
        let list = listFunDataset;
        if (list === null) {
            list = defaultListFun;
        }
        const { getter, lister } = createSewerRatFunctions(url, get, list);
        super(path, getter, lister);
    }
}
