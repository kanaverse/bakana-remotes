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

async function createTakaneFunctions(url, prefix, path, get) {
    const overall = url + "/" + prefix;
    if (path !== null) {
        overall += "/" + path;
    }
    const raw_man = await get(overall + "/..manifest");
    const dec = new TextDecoder;
    const man = JSON.parse(dec.decode(raw_man));

    const contents = {};
    for (const k of Object.keys(man)) {
        const comp = k.split("/");
        let step = prefix;
        for (var i = 0; i < comp.length; i++) {
            if (!(step in contents)) {
                contents[step] = new Set;
            }
            contents[step].add(comp[i]);
            step += "/" + comp[i];
        }
    }

    for (const [k, v] of Object.entries(contents)) {
        contents[k] = Array.from(v);
    }

    const getter = p => get(url + "/" + p);
    const lister = p => {
        if (!(p in contents)) {
            throw new Error("no directory listing available for '" + p + "'");
        }
        return contents[p];
    };
    return { getter, lister };
}

/**
 * Dataset represented by a SummarizedExperiment in the [**gypsum**](https://github.com/ArtifactDB/gypsum-worker) store.
 * This extends the [AbstractDataset](https://kanaverse.github.io/bakana-takane/AbstractDataset.html) class.
 * @hideconstructor
 */
export class GypsumDataset extends bt.AbstractDataset {
    #id;

    /**
     * @param {function} get - A (possibly `async`) function that accepts a URL and returns a Uint8Array of that URL's contents.
     * Alternatively `null`, to reset the function to its default value based on `fetch`.
     * @return {?function} Previous setting of the GET function.
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
     * @param {string} [url=https://data-gypsum.artifactdb.com] - URL to the **gypsum** bucket contents.
     */
    static async create(project, asset, version, path, url = "https://data-gypsum.artifactdb.com") {
        let get = getFunDataset;
        if (get === null) {
            get = defaultGetFun;
        }
        const { getter, lister } = await createTakaneFunctions(url, project + "/" + asset + "/" + version, path, get);
        return new GypsumDataset(project, asset, version, path, url, getter, lister);
    }

    constructor(project, asset, version, path, url, getter, lister) {
        let combined = project + "/" + asset + "/" + version;
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
            throw new Error("expected a file of type 'id' when unserializing CollaboratorDB dataset"); 
        }
        const id = JSON.parse(args.id);

        let output = await GypsumDataset.create(id.project, id.asset, id.version, id.path, id.url);
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
     * @return {?function} Previous setting of the GET function.
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
     * @param {string} [url=https://data-gypsum.artifactdb.com] - URL to the **gypsum** bucket contents.
     */
    static async create(project, asset, version, path, url = "https://data-gypsum.artifactdb.com") {
        let get = getFunResult;
        if (get === null) {
            get = defaultGetFun;
        }
        const { getter, lister } = await createTakaneFunctions(url, project + "/" + asset + "/" + version, path, get);
        return new GypsumResult(project, asset, version, path, url, getter, lister);
    }

    constructor(project, asset, version, path, url, getter, lister) {
        let combined = project + "/" + asset + "/" + version;
        if (path !== null) {
            combined += "/" + path;
        }
        super(combined, getter, lister);
    }
}
