import * as adb from "artifactdb";
import * as bakana from "bakana";
import * as scran from "scran.js";
import * as bioc from "bioconductor";
//import * as chihaya from "chihaya";

function detect_csv_compression(details) {
    if ("csv_data_frame" in details) {
        let csv_info = details.csv_data_frame;
        if ("compression" in csv_info) {
            if (csv_info.compression == "gzip") {
                return "gz";
            } else if (csv_info.compression != "none") {
                throw new Error("compression '" + csv_info.compression + "' is not supported");
            }
        }
    }
    return "none";
}

/**
 * Load a {@linkplain external:DataFrame} given an ArtifactDB ID.
 * Non-atomic columns are currently skipped, and factors/dates are treated as string arrays.
 *
 * @param {string} baseUrl - Base URL of the ArtifactDB instance.
 * @param {string} id - Identifier for the data frame.
 * @param {object} [options={}] - Optional parameters.
 * @param {?function} [options.getFun=null] - Function that accepts a URL string and performs a GET to return a Response object,
 * see [`getFileMetadata`](https://artifactdb.github.io/artifactdb.js/global.html#getFileMetadata) for details.
 * @param {?function} [options.downloadFun=null] - Function that accepts a URL string and downloads the resource, returning either a file path (string) or an ArrayBuffer containing the file contents;
 * see [`getFile`](https://artifactdb.github.io/artifactdb.js/global.html#getFile) for details.
 *
 * @return {external:DataFrame} The DataFrame, absent any "other" columns.
 */
export async function loadDataFrame(baseUrl, id, { getFun = null, downloadFun = null } = {}) {
    let details = await adb.getFileMetadata(baseUrl, id, { getFun });
    let unpacked = adb.unpackId(id);
    let content = await adb.getFile(baseUrl, adb.packId(unpacked.project, details.path, unpacked.version), { getFun, downloadFun });

    let headers = [];
    let columns = [];
    let row_names = null;

    let schema = details["$schema"];
    if (schema.startsWith("csv_data_frame/") || "csv_data_frame" in details) {
        let compression = detect_csv_compression(details);
        let parsed = await bakana.readTable2(content, { compression: compression, delim: ",", chunkSize: 65536 });
        headers = parsed.shift(); // remove the header.

        for (var i = 0; i < headers.length; i++) {
            let vectorized = null;
            try {
                vectorized = parsed.map(r => r[i]);
            } catch (e) {
                console.warn("failed to parse column " + String(i + 1) + " of the data frame '" + details._extra.id + "'");
            }
            columns.push(vectorized);
        }

        if (details.data_frame.row_names) {
            row_names = columns.shift();
            headers.shift();
        }

    } else if (schema.startsWith("hdf5_data_frame/") || "hdf5_data_frame" in details) {
        let content = await download2(handle);
        let realized = scran.realizeFile(content);
        try {
            let fhandle = new scran.H5File(realized.path);
            headers = fhandle.open("column_names", { load: true }).values;

            if ("row_names" in fhandle.children) {
                row_names = fhandle.open("row_names", { load: true }).values;
            }

            let dhandle = fhandle.open("data");
            for (var i = 0; i < headers.length; i++) {
                columns.push(dhandle.open(String(i), { load: true }).values);
            }
        } finally {
            realized.flush();
        }

    } else {
        throw new Error("schema '" + schema + "' is not yet supported for Data Frames");
    }

    // Coercing type based on the column metadata.
    let col_info = details.data_frame.columns;

    if (col_info.length == columns.length) { 
        for (var c = 0; c < columns.length; c++) {
            let anno = col_info[c];
            if (anno.type == "other") {
                columns[c] = null;
            }
            if (columns[c] === null) {
                continue;
            }

            let current = columns[c];
            if (anno.type == "number" || anno.type == "integer") {
                if (current instanceof Array) {
                    // Convert from string arrays to TypedArrays.
                    let numeric = bakana.promoteToNumber(current);
                    if (numeric !== null) {
                        current = numeric;
                    }
                }
            } else if (anno.type == "boolean") {
                if (current instanceof Array) {
                    // Convert from string array to boolean array.
                    current.forEach((x, i) => {
                        let z = x.toLowerCase();
                        if (z == "true") {
                            current[i] = true;
                        } else if (z == "false") {
                            current[i] = false;
                        } else {
                            current[i] = null;
                        }
                    });
                } else {
                    // Convert from TypedArray to boolean array.
                    let replacement = [];
                    for (const x of current) {
                        if (x == -2147483648) {
                            replacement.push(null);
                        } else {
                            replacement.push(x != 0);
                        }
                    }
                    current = replacement;
                }
            }

            headers[c] = anno.name;
            columns[c] = current;
        }

    } else {
        console.warn("skipped type checks due to mismatching column number for '" + details._extra.id + "'");
        for (var c = 0; c < columns.length; c++) {
            let current = columns[c];

            // Just auto-guessing the conversion from string arrays to TypedArrays.
            if (current instanceof Array) {
                let numeric = bakana.promoteToNumber(current);
                if (numeric !== null) {
                    columns[c] = numeric;
                }
            }
        }
    }

    // Actually creating a DataFrame.
    let reheaders = [];
    let recolumns = {};
    for (var c = 0; c < columns.length; c++) {
        if (columns[c] === null) {
            continue;
        }
        let h = headers[c];
        reheaders.push(h);
        recolumns[h] = columns[c];
    }

    return new bioc.DataFrame(recolumns, { rowNames: row_names, columnOrder: reheaders });
}

/**
 * Load a sparse {@linkplain external:ScranMatrix} given an ArtifactDB ID.
 * This currently supports loading from `hdf5_dense_array`, `hdf5_sparse_matrix` and `hdf5_delayed_array` instances.
 *
 * @param {string} baseUrl - Base URL of the ArtifactDB instance.
 * @param {string} id - Identifier for the data frame.
 * @param {object} [options={}] - Optional parameters.
 * @param {?function} [options.getFun=null] - Function that accepts a URL string and performs a GET to return a Response object,
 * see [`getFileMetadata`](https://artifactdb.github.io/artifactdb.js/global.html#getFileMetadata) for details.
 * @param {?function} [options.downloadFun=null] - Function that accepts a URL string and downloads the resource, returning either a file path (string) or an ArrayBuffer containing the file contents;
 * see [`getFile`](https://artifactdb.github.io/artifactdb.js/global.html#getFile) for details.
 * @param {boolean} [options.forceInteger=true] - Whether to force an integer representation for the matrix data.
 * This saves space when floating-point is unnecessary. 
 * @param {boolean} [options.layered=true] - Whether to use a layered representation for the sparse matrix.
 * This saves space but adds complexity in downstream analysis by permuting the rows.
 *
 * @return {object} An object containing:
 * - `matrix`, a {@linkplain external:ScranMatrix} containing the contents of the matrix.
 * - `row_ids`, an Int32Array containing the row identities, possibly permuted when `layered = true`.
 */
export async function loadScranMatrix(baseUrl, id, { getFun = null, downloadFun = null, forceInteger = true, layered = true } = {} ) {
    let details = await adb.getFileMetadata(baseUrl, id, { getFun });
    let unpacked = adb.unpackId(id);
    let content = await adb.getFile(baseUrl, adb.packId(unpacked.project, details.path, unpacked.version), { getFun, downloadFun });

    let schema = details["$schema"]
    let is_dense = (schema.startsWith("hdf5_dense_array/") || "hdf5_dense_array" in details);
    let is_sparse = (schema.startsWith("hdf5_sparse_matrix/") || "hdf5_sparse_matrix" in details);
    let output = {};

    let mock_consecutive_ids = NR => {
        let dummy = new Int32Array(NR);
        dummy.forEach((x, i) => { dummy[i] = i; });
        return dummy;
    };

    if (is_dense || is_sparse) {
        let group = (is_dense ? details.hdf5_dense_array.dataset : details.hdf5_sparse_matrix.group);
        let finfo = scran.realizeFile(content);
        try {
            output = scran.initializeSparseMatrixFromHDF5(finfo.path, group, { forceInteger: forceInteger, layered: layered });
            if (output.row_ids == null) {
                output.row_ids = mock_consecutive_ids(output.matrix.numberOfRows());
            }
        } finally {
            finfo.flush();
        }

//    } else if (schema.startsWith("hdf5_delayed_array/") || "hdf5_delayed_array" in details) {
//        let finfo = scran.realizeFile(contents);
//        try {
//            output.matrix = await chihaya.load(finfo.path, details.hdf5_delayed_array.group);
//            if (output.row_ids == null) {
//                output.row_ids = mock_consecutive_ids(output.matrix.numberOfRows());
//            }
//        } finally {
//            finfo.flush();
//        }

    } else {
        throw new Error("only supporting 'hdf5_dense_matrix', 'hdf5_sparse_matrix' or 'hdf5_delayed_array' objects right now");
    }

    let expected_dims = details["array"]["dimensions"];
    let loaded = output.matrix;
    if (loaded.numberOfRows() != expected_dims[0] || loaded.numberOfColumns() != expected_dims[1]) {
        scran.free(loaded);
        throw new Error("dimensions of the matrix is not consistent with its metadata");
    }

    return output;
}
