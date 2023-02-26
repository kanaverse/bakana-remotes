import * as scran from "scran.js";
import * as bioc from "bioconductor";
import * as bakana from "bakana";
import * as utils from "./utils.js";

const baseUrl = "https://experimenthub.bioconductor.org/fetch";

const registry = {
    "zeisel-brain": { "counts": "2596", "coldata": "2598", "rowdata": "2597" }, // corresponding to EH2580, 2582 and 2581, for whatever reason.
    "segerstolpe-pancreas": { "counts": "2591", "coldata": "2593", "rowdata": "2592" }, // corresponding to EH2575, 2577 and 2576.
    "nestorowa-hsc": { "counts": "2710", "ncol": 1920 }, // corresponding to EH2694; the coldata doesn't contain much that's useful here.
    "aztekin-tail": { "counts": "3124", "coldata": "3125" }, // corresponding to EH3108 and 3109.
    "wu-kidney": { "counts": "3594", "coldata": "3595" }, // corresponding to EH3558 and 3559.
    "zilionis-mouse-lung": { "counts": "3478", "coldata": "3479" } // corresponding to EH3462 and 3463.
};

function check_class(handle, accepted, base) {
    if (!(handle instanceof scran.RdsS4Object)) {
        throw new Error("expected an S4 object");
    }

    for (const [k, v] of Object.entries(accepted)) {
        if (handle.className() == k && handle.packageName() == v) {
            return;
        }
    }
    throw new Error("object is not a " + base + " or one of its recognized subclasses");
}

function load_listData_names(lhandle) {
    let ndx = lhandle.findAttribute("names");
    if (ndx < 0) {
        return null;
    }

    let nhandle;
    let names;
    try {
        nhandle = lhandle.attribute(ndx);
        names = nhandle.values();
    } catch(e) {
        throw new Error("failed to load listData names; " + e.message);
    } finally {
        scran.free(nhandle);
    }

    if (names.length != lhandle.length()) {
        throw new Error("expected names to have same length as listData");
    }
    return names;
}

function populate_list_columns(lhandle, output) {
    let colnames = load_listData_names(lhandle);
    if (colnames == null) {
        throw new Error("expected the listData list to be named");
    }
    let columns = {};

    for (var i = 0; i < lhandle.length(); i++) {
        let curhandle;
        try {
            curhandle = lhandle.load(i);
            if (curhandle instanceof scran.RdsVector && !(curhandle instanceof scran.RdsGenericVector)) {
                let curcol = curhandle.values();
                columns[colnames[i]] = curcol;
                output.nrow = curcol.length;
            }
        } finally {
            scran.free(curhandle);
        }
    }

    output.columns = columns;
    return;
}

function load_data_frame(handle) {
    let output = {};

    if (handle.type() == "S4") {
        check_class(handle, { "DFrame": "S4Vectors", "DataFrame": "S4Vectors" }, "DFrame");

        // Loading the atomic columns.
        let lhandle;
        try {
            lhandle = handle.attribute("listData");
            if (!(lhandle instanceof scran.RdsGenericVector)) {
                throw new Error("listData slot should be a generic list");
            }
            populate_list_columns(lhandle, output);
        } catch(e) {
            throw new Error("failed to retrieve data from DataFrame's listData; " + e.message);
        } finally {
            scran.free(lhandle);
        }
    } else {
        if (handle.type() != "vector") {
            throw new Error("expected a data.frame or DataFrame instance");
        }
        populate_list_columns(handle, output);
    }

    // Loading the row names.
    let rnhandle;
    try {
        let rndx = handle.findAttribute("rownames");
        if (rndx >= 0) {
            rnhandle = handle.attribute(rndx);
            if (rnhandle instanceof scran.RdsStringVector) {
                output.row_names = rnhandle.values();
                output.nrow = output.row_names.length;
            }
        }
    } catch(e) {
        throw new Error("failed to retrieve row names from DataFrame; " + e.message);
    } finally {
        scran.free(rnhandle);
    }

    // Loading the number of rows.
    if (!("nrow" in output)) {
        let nrhandle;
        try {
            nrhandle = handle.attribute("nrows");
            if (!(nrhandle instanceof scran.RdsIntegerVector)) {
                throw new Error("expected an integer vector as the 'nrows' slot");
            }
            let NR = nrhandle.values();
            if (NR.length != 1) {
                throw new Error("expected an integer vector of length 1 as the 'nrows' slot");
            }
            output.nrow = NR[0];
        } catch (e) {
            throw new Error("failed to retrieve nrows from DataFrame; " + e.message);
        } finally {
            scran.free(nrhandle);
        }
    }

    return output;
}

function extract_matrix_rownames(handle) {
    let idx;

    if (handle.type() == "S4") {
        check_class(handle, { "dgCMatrix": "Matrix", "dgTMatrix": "Matrix" }, "Matrix");
        idx = handle.findAttribute("Dimnames");
    } else {
        idx = handle.findAttribute("dimnames");
    }

    if (idx < 0) {
        throw new Error("count matrix does not have dimnames");
    }

    let dimhandle;
    let firsthandle;
    let output;
    
    try {
        dimhandle = handle.attribute(idx);
        if (dimhandle.type() != "vector" && dimhandle.size() != 2) {
            throw new Error("dimnames of the count matrix should be a list of length 2");
        }

        firsthandle = dimhandle.load(0);
        if (firsthandle.type() != "string") {
            throw new Error("expected a character vector in the first dimnames");
        }

        output = firsthandle.values();
    } finally {
        scran.free(dimhandle);
        scran.free(firsthandle);
    }

    return output;
}

/**
 * Dataset derived from a SummarizedExperiment-like representation on Bioconductor's [ExperimentHub](https://bioconductor.org/packages/ExperimentHub).
 */
export class ExperimentHubDataset {
    #id;

    #rowdata;
    #coldata;

    #counts_handle;
    #counts_loaded;

    // We should _know_ which experiments correspond to which modality for each
    // dataset in our registry, so there's no need to provide options for that.
    // However, we might not know how to choose an appropriate primary
    // identifier for combining datasets, hence these options.
    #primaryRnaFeatureIdColumn;

    #dump_options() {
        return {
            primaryRnaFeatureIdColumn: this.#primaryRnaFeatureIdColumn
        };
    }

    /****************************************
     ****************************************/

    static #downloadFun = async url => {
        let resp = await fetch(url);
        if (!resp.ok) {
            throw new Error("failed to fetch content at " + url + " (" + resp.status + ")");
        }
        return new Uint8Array(await resp.arrayBuffer());
    }

    /** 
     * @param {function} fun - Function that accepts a URL string and downloads the resource,
     * returning a Uint8Array of its contents.
     * Alternatively, on Node.js, the funciton may return a string containing the path to the downloaded resource.
     * @return {function} Previous setting of the download function.
     */
    static setDownloadFun(fun) {
        let previous = ExperimentHubDataset.#downloadFun;
        ExperimentHubDataset.#downloadFun = fun;
        return previous;
    }

    /****************************************
     ****************************************/

    /**
     * @return {Array} Array of strings containing identifiers of available datasets.
     * @static
     */
    static availableDatasets() {
        return Object.keys(registry);
    }

    /**
     * @param {string} id - Identifier of a dataset to load.
     * This should be a string in {@linkcode ExperimentHubDataset.availableDatasets availableDatasets}.
     * @param {object} [options={}] - Optional parameters.
     * @param {string|number} [options.primaryRnaFeatureIdColumn=0] - See {@linkcode TenxHdf5Dataset#setPrimaryRnaFeatureIdColumn setPrimaryRnaFeatureIdColumn}.
     */
    constructor(id, { 
        primaryRnaFeatureIdColumn = 0
    } = {}) {
        this.#id = id;
        if (!(this.#id in registry)) {
            throw new Error("unrecognized identifier '" + this.#id + "' for ExperimentHub-based datasets");
        }

        this.#primaryRnaFeatureIdColumn = primaryRnaFeatureIdColumn;

        this.clear();
    }

    /**
     * @return {string} Format of this dataset class.
     * @static
     */
    static format() {
        return "ExperimentHub";
    }

    /**
     * Destroy caches if present, releasing the associated memory.
     * This may be called at any time but only has an effect if `cache = true` in {@linkcode ExperimentHubDataset#load load} or {@linkcodeExperimentHubDataset#annotations annotations}. 
     */
    clear() {
        scran.free(this.#counts_handle);
        scran.free(this.#counts_loaded);
        this.#counts_handle = null;
        this.#counts_loaded = null;
        this.#rowdata = null;
        this.#coldata = null;
    }

    /**
     * @return {object} Object containing the abbreviated details of this dataset.
     */
    abbreviate() {
        return { "id": this.#id, "options": this.#dump_options() };
    }

    /**
     * @param {string|number} i - Name or index of the column of the `features` {@linkplain external:DataFrame DataFrame} that contains the primary feature identifier for gene expression.
     * If `i` is invalid (e.g., out of range index, unavailable name), it is ignored and the primary identifier is treated as undefined.
     */
    setPrimaryRnaFeatureIdColumn(i) {
        this.#primaryRnaFeatureIdColumn = i;
        return;
    }

    async #counts() {
        if (this.#counts_handle !== null) {
            return;
        }

        let details = registry[this.#id];
        let counts_deets = await ExperimentHubDataset.#downloadFun(baseUrl + "/" + details.counts);
        try {
            this.#counts_loaded = scran.readRds(counts_deets);
            this.#counts_handle = this.#counts_loaded.value();
        } catch(e) {
            scran.free(this.#counts_handle);
            scran.free(this.#counts_loaded);
            throw e;
        }
    }

    async #features() {
        if (this.#rowdata !== null) {
            return;
        }

        let details = registry[this.#id];

        if ("rowdata" in details) {
            let rowdata_deets = await ExperimentHubDataset.#downloadFun(baseUrl + "/" + details.rowdata);

            let rowdata_load;
            let rowdata_handle;
            try {
                rowdata_load = scran.readRds(rowdata_deets);
                rowdata_handle = rowdata_load.value();
                let rowdata = load_data_frame(rowdata_handle);
                let names = rowdata.row_names;

                let output = {};
                if (names) {
                    output.id = names;
                }

                for (const [k, v] of Object.entries(rowdata.columns)) {
                    if (k.match(/^sym/)) {
                        output[k] = v;
                    }
                }

                if (Object.keys(output).length == 0) {
                    throw new Error("no acceptable feature identifiers found in the rowData DataFrame");
                }
                this.#rowdata = new bioc.DataFrame(output);
            } finally {
                scran.free(rowdata_handle);
                scran.free(rowdata_load);
            }
            return;
        }

        // Otherwise we pull the details from the counts.
        await this.#counts();
        let ids = extract_matrix_rownames(this.#counts_handle);
        this.#rowdata = new bioc.DataFrame({ id: ids });
    }

    async #cells() {
        if (this.#coldata !== null) {
            return;
        }

        let details = registry[this.#id];
        if ("coldata" in details) {
            let coldata_deets = await ExperimentHubDataset.#downloadFun(baseUrl + "/" + details.coldata);

            let coldata_load;
            let coldata_handle; 
            let cd_df;
            try {
                coldata_load = scran.readRds(coldata_deets);
                coldata_handle = coldata_load.value();
                cd_df = load_data_frame(coldata_handle);
            } finally {
                scran.free(coldata_handle);
                scran.free(coldata_load);
            }

            this.#coldata = new bioc.DataFrame(cd_df.columns, { numberOfRows: cd_df.nrow });
        } else {
            this.#coldata = new bioc.DataFrame({}, { numberOfRows: details.ncol });
        }

        return;
    }

    /**
     * @param {object} [options={}] - Optional parameters.
     * @param {boolean} [options.cache=false] - Whether to cache the results for re-use in subsequent calls to this method or {@linkcode ExperimentHubDataset#load load}.
     * If `true`, users should consider calling {@linkcode ExperimentHubDataset#clear clear} to release the memory once this dataset instance is no longer needed.
     * 
     * @return {object} Object containing the per-feature and per-cell annotations.
     * This has the following properties:
     *
     * - `modality_features`: an object where each key is a modality name and each value is a {@linkplain external:DataFrame DataFrame} of per-feature annotations for that modality.
     *   Unlike {@linkcode ExperimentHubDataset#load load}, modality names are arbitrary.
     * - `cells`: a {@linkplain external:DataFrame DataFrame} of per-cell annotations.
     */
    async summary({ cache = false } = {}) {
        await this.#features();
        await this.#cells();

        let output = { cells: utils.cloneCached(this.#coldata, cache) };
        let my_rd = utils.cloneCached(this.#rowdata, cache);
        output.modality_features = { "RNA": my_rd };

        if (!cache) {
            this.clear();
        }
        return output;
    }

    /**
     * @param {object} [options={}] - Optional parameters.
     * @param {boolean} [options.cache=false] - Whether to cache the results for re-use in subsequent calls to this method or {@linkcode TenxHdf5Dataset#summary summary}.
     * If `true`, users should consider calling {@linkcode TenxHdf5Dataset#clear clear} to release the memory once this dataset instance is no longer needed.
     *
     * @return {object} Object containing the per-feature and per-cell annotations.
     * This has the following properties:
     *
     * - `features`: an object where each key is a modality name and each value is a {@linkplain external:DataFrame DataFrame} of per-feature annotations for that modality.
     * - `cells`: a {@linkplain external:DataFrame DataFrame} containing per-cell annotations.
     * - `matrix`: a {@linkplain external:MultiMatrix MultiMatrix} containing one {@linkplain external:ScranMatrix ScranMatrix} per modality.
     * - `primary_ids`: an object where each key is a modality name and each value is an integer array containing the feature identifiers for each row in that modality.
     *
     * Modality names are guaranteed to be one of `"RNA"`, `"ADT"` or `"CRISPR"`.
     * It is assumed that an appropriate mapping from the feature types inside the `featureFile` was previously declared,
     * either in the constructor or in setters like {@linkcode setFeatureTypeRnaName}.
     */
    async load({ cache = false } = {}) {
        await this.#features();
        await this.#cells();
        await this.#counts();

        let output = {
            cells: utils.cloneCached(this.#coldata, cache)
        };

        // Hard-coding the fact that we're dealing with RNA here, as all
        // registry entries are currently RNA-only anyway.
        let details = registry[this.#id];
        try {
            output.matrix = new scran.MultiMatrix;
            let counts = scran.initializeSparseMatrixFromRds(this.#counts_handle, { consume: !cache });

            output.matrix.add("RNA", counts.matrix);
            output.row_ids = { "RNA": counts.row_ids };

            let perm_features = bioc.SLICE(this.#rowdata, counts.row_ids);
            output.features = { "RNA": perm_features };
        } catch (e) {
            scran.free(output.matrix);
            throw e;
        }

        // Setting the primary identifiers.
        let curfeat = output.features["RNA"];
        let id = this.primaryRnaFeatureIdColumn;
        output.primary_ids = { RNA: null }; 
        if ((typeof id == "string" && curfeat.hasColumn(id)) || (typeof id == "number" && id < curfeat.numberOfColumns())) {
            output.primary_ids.RNA = curfeat.column(id);
        } else {
            output.primary_ids.RNA = curfeat.rowNames();
        }

        if (!cache) {
            this.clear();
        }
        return output;
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
            options: this.#dump_options()
        }
    }

    /**
     * @param {Array} files - Array of objects like that produced by {@linkcode ExperimentHubDataset#serialize serialize}.
     * @param {object} options - Object containing additional options to be passed to the constructor.
     * @return {ExperimentHubDataset} A new instance of this class.
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
            throw new Error("expected a file of type 'id' when unserializing ExperimentHub dataset"); 
        }
        return new ExperimentHubDataset(args.id, options);
    }
}
