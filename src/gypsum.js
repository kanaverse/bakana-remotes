import * as scran from "scran.js";
import * as bioc from "bioconductor";
import * as bakana from "bakana";
import * as bt from "bakana-takane";
import * as utils from "./utils.js";

var getFunDataset = null;
var getFunResult = null;

async function defaultGetFun(u) {
    const res = await fetch(u);
    if (res.ok) {
        const err = Error("failed to fetch '" + u + "'");
        err.status_code = res.status_code;
        return err;
    }
    return new Uint8Array(await res.arrayBuffer());
}

function createTakaneFunctions(url, prefix, path, get) {
    const getter = p => get(url + "/file/" + encodeURIComponent(p));

    var listing = null;
    const lister = async p => {
        if (listing == null) {
            const manpath = url + "/file/" + encodeURIComponent(prefix + "/..manifest");
            const raw_man = await get(manpath);
            const dec = new TextDecoder;
            const man = JSON.parse(dec.decode(raw_man));

            listing = {};
            for (const k of Object.keys(man)) {
                const comp = k.split("/");
                let step = prefix;
                for (var i = 0; i < comp.length; i++) {
                    if (!(step in listing)) {
                        listing[step] = new Set;
                    }
                    listing[step].add(comp[i]);
                    step += "/" + comp[i];
                }
            }

            for (const [k, v] of Object.entries(listing)) {
                listing[k] = Array.from(v);
            }
        }

        if (!(p in listing)) {
            throw new Error("no directory listing available for '" + p + "'");
        }
        return listing[p];
    };

    return { getter, lister };
}

/**
 * Dataset represented by a SummarizedExperiment in the [**gypsum**](https://github.com/ArtifactDB/gypsum-worker) store.
 * This extends the [AbstractDataset](https://kanaverse.github.io/bakana-takane/AbstractDataset.html) class.
 */
export class GypsumDataset extends bt.AbstractDataset {
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
     * @param {string} project - Name of the project.
     * @param {string} asset - Name of the asset.
     * @param {string} version - Name of the version.
     * @param {?string} path - Path to the SummarizedExperiment inside the version directory.
     * This can be `null` if the SummarizedExperiment exists at the root of the directory.
     * @param {string} [url=https://gypsum.artifactdb.com] - URL to the **gypsum** REST API.
     */
    constructor(project, asset, version, path, url = "https://gypsum.artifactdb.com") {
        let get = getFunDataset;
        if (get === null) {
            get = defaultGetFun;
        }
        let combined = project + "/" + asset + "/" + version;
        const { getter, lister } = createTakaneFunctions(url, combined, path, get);
        if (path !== null) {
            combined += "/" + path;
        }
        super(combined, getter, lister);
        this.#id = { project, asset, version, path, url };
    }

    /**
     * @return {string} Format of this dataset class.
     * @static
     */
    static format() {
        return "gypsum";
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
     * @param {Array} files - Array of objects like that produced by {@linkcode GypsumDataset#serialize serialize}.
     * @param {object} options - Object containing additional options to be passed to the constructor.
     * @return {GypsumDataset} A new instance of this class.
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
            throw new Error("expected a file of type 'id' when unserializing a GypsumDataset"); 
        }
        const id = JSON.parse(args.id);

        let output = new GypsumDataset(id.project, id.asset, id.version, id.path, id.url);
        output.setOptions(options);
        return output;
    }
}

/**
 * Result represented as a SummarizedExperiment in the [**gypsum**](https://github.com/ArtifactDB/gypsum-worker) store.
 * This extends the [AbstractResult](https://kanaverse.github.io/bakana-takane/AbstractResult.html) class.
 * @hideconstructor
 */
export class GypsumResult extends bt.AbstractResult {
    /**
     * @param {function} get - A (possibly `async`) function that accepts a URL and returns a Uint8Array of that URL's contents.
     * Alternatively `null`, to reset the function to its default value based on `fetch`.
     * @return {?function} Previous setting of the getter function.
     */
    static setGetFun(fun) {
        let previous = getFunResult;
        getFunResult = fun;
        return previous;
    }

    /**
     * @param {string} project - Name of the project.
     * @param {string} asset - Name of the asset.
     * @param {string} version - Name of the version.
     * @param {?string} path - Path to the SummarizedExperiment inside the version directory.
     * This can be `null` if the SummarizedExperiment exists at the root of the directory.
     * @param {string} [url=https://gypsum.artifactdb.com] - URL to the **gypsum** REST API.
     */
    constructor(project, asset, version, path, url = "https://gypsum.artifactdb.com") {
        let get = getFunResult;
        if (get === null) {
            get = defaultGetFun;
        }
        let combined = project + "/" + asset + "/" + version;
        const { getter, lister } = createTakaneFunctions(url, combined, path, get);
        if (path !== null) {
            combined += "/" + path;
        }
        super(combined, getter, lister);
    }
}
