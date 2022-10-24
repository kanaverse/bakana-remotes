import * as scran from "scran.js";
import * as bakana from "bakana";
import * as utils from "./utils.js";

const baseUrl = "https://experimenthub.bioconductor.org/fetch";

const registry = {
    "zeisel-brain": { "counts": "2596", "coldata": "2598", "rowdata": "2597" } // corresponding to EH2580, 2582 and 2581, for whatever reason.
};

export function abbreviate(args) {
    return {
        "format": "ExperimentHub", 
        "id": args.id
    };
}

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

function load_data_frame(handle) {
    check_class(handle, { "DFrame": "S4Vectors", "DataFrame": "S4Vectors" }, "DFrame");
    let output = {};

    // Loading the atomic columns.
    let columns = {};
    let lhandle;
    try {
        lhandle = handle.attribute("listData");
        if (!(lhandle instanceof scran.RdsGenericVector)) {
            throw new Error("listData slot should be a generic list");
        }

        let colnames = load_listData_names(lhandle);
        if (colnames == null) {
            throw new Error("expected the listData list to be named");
        }

        for (var i = 0; i < lhandle.length(); i++) {
            let curhandle;
            try {
                curhandle = lhandle.load(i);
                if (curhandle instanceof scran.RdsVector && !(curhandle instanceof scran.RdsGenericVector)) {
                    columns[colnames[i]] = curhandle.values();
                }
            } finally {
                scran.free(curhandle);
            }
        }
    } catch(e) {
        throw new Error("failed to retrieve data from DataFrame's listData; " + e.message);
    } finally {
        scran.free(lhandle);
    }
    output.columns = columns;

    // Loading the row names.
    let rnhandle;
    try {
        rnhandle = handle.attribute("rownames");
        if (rnhandle instanceof scran.RdsStringVector) {
            output.row_names = rnhandle.values();
        }
    } catch(e) {
        throw new Error("failed to retrieve row names from DataFrame; " + e.message);
    } finally {
        scran.free(rnhandle);
    }

    return output;
}

function extract_features(handle) {
    let rowdata = load_data_frame(handle);
    let names = rowdata.row_names;
    if (!names) {
        throw new Error("no row names found in the rowData dataframe");
    }

    let output = { id: names };
    for (const [k, v] of Object.entries(rowdata.columns)) {
        if (k.match(/^sym/)) {
            output[k] = v;
        }
    }

    return output;
}

export async function preflight(args) {
    let id = args.id;
    if (!(id in registry)) {
        throw new Error("unrecognized identifier '" + id + "' for ExperimentHub-based datasets");
    }
    let details = registry[id];

    let output_anno = {};
    {
        let coldata_deets = await utils.downloadFun(baseUrl + "/" + details.coldata);
        let coldata_load;
        let coldata_handle; 
        let cd_df;
        try {
            coldata_load = scran.readRds(new Uint8Array(coldata_deets));
            coldata_handle = coldata_load.value();
            cd_df = load_data_frame(coldata_handle);
        } finally {
            scran.free(coldata_handle);
            scran.free(coldata_load);
        }
        for (const [k, v] of Object.entries(cd_df.columns)) {
            output_anno[k] = bakana.summarizeArray(v);
        }
    }

    let output_feat = {};
    {
        let rowdata_deets = await utils.downloadFun(baseUrl + "/" + details.rowdata);
        let rowdata_load;
        let rowdata_handle;
        try {
            rowdata_load = scran.readRds(new Uint8Array(rowdata_deets));
            rowdata_handle = rowdata_load.value();
            output_feat.RNA = extract_features(rowdata_handle);
        } finally {
            scran.free(rowdata_handle);
            scran.free(rowdata_load);
        }
    }

    return { annotations: output_anno, genes: output_feat };
}

export class Reader {
    #id;

    constructor(args, formatted = false) {
        this.#id = args.id;
        if (!(this.#id in registry)) {
            throw new Error("unrecognized identifier '" + this.#id + "' for ExperimentHub-based datasets");
        }
    }

    async load() {
        let details = registry[this.#id];
        let output = {};

        let coldata_deets = await utils.downloadFun(baseUrl + "/" + details.coldata);
        let coldata_load;
        let coldata_handle;
        try {
            coldata_load = scran.readRds(new Uint8Array(coldata_deets));
            coldata_handle = coldata_load.value();
            output.annotations = load_data_frame(coldata_handle).columns;
        } finally {
            scran.free(coldata_handle);
            scran.free(coldata_load);
        }

        let rowdata_deets = await utils.downloadFun(baseUrl + "/" + details.rowdata);
        let rowdata_load;
        let rowdata_handle;
        try {
            rowdata_load = scran.readRds(new Uint8Array(rowdata_deets));
            rowdata_handle = rowdata_load.value();
            output.genes = { RNA: extract_features(rowdata_handle) };
        } finally {
            scran.free(rowdata_handle);
            scran.free(rowdata_load);
        }

        let counts_deets = await utils.downloadFun(baseUrl + "/" + details.counts);
        let counts_load;
        let counts_handle;
        try {
            output.matrix = new scran.MultiMatrix;
            let counts_load = scran.readRds(new Uint8Array(counts_deets));
            let counts_handle = counts_load.value();
            let counts = scran.initializeSparseMatrixFromRds(counts_handle, { consume: true });

            output.matrix.add("RNA", counts.matrix);
            if (counts.row_ids) {
                for (const [k, v] of Object.entries(output.genes.RNA)) {
                    output.genes.RNA[k] = scran.quickSliceArray(counts.row_ids, v);
                }
            }
        } catch (e) {
            scran.free(counts_handle);
            scran.free(counts_load);
            scran.free(output.matrix);
            throw e;
        }

        return output;
    }

    format() {
        return "ExperimentHub";
    }

    async serialize(embeddedSaver) {
        let output = {
            type: "id",
            name: "id"
        };

        const enc = new TextEncoder;
        let buffer = enc.encode(this.#id);
        let eout = embeddedSaver(buffer.buffer, buffer.length);
        output.offset = eout.offset;
        output.size = eout.size;

        return [ output ];
    }
}

export async function unserialize(values, embeddedLoader) {
    let args = {};

    // This should contain 'id'.
    for (const x of values) {
        let id2 = await embeddedLoader(x.offset, x.size);
        if (typeof id2 == "string") {
            throw new Error("not yet supported!"); // TODO: expose bakana's abstract LoadedFile to handle this.
        } else {
            const dec = new TextDecoder;
            args[x.type] = dec.decode(new Uint8Array(id2));
        }
    }

    return new Reader(args, true);
}
