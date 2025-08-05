import * as fs from "fs";
import * as bakana from "bakana";
import * as bioc from "bioconductor";

export async function initializeAll() {
    await bakana.initialize({ localFile: true });
}

export async function downloader(url) {
    if (!fs.existsSync("files")) {
        fs.mkdirSync("files");
    }
    let path = "files/" + encodeURIComponent(url);

    if (!fs.existsSync(path)) {
        let res = await fetch(url);
        if (!res.ok) {
            throw new Error("failed to fetch resource at '" + url + "' (" + String(res.status) + ")");
        }

        let contents = new Uint8Array(await res.arrayBuffer());
        fs.writeFileSync(path, contents);
        return contents;
    }

    // Node.js buffers are Uint8Arrays, so we can just return it without casting,
    // though we need to make a copy to avoid overwriting.
    return fs.readFileSync(path).slice();
}

/***********************************/

// Copied from the bakana tests... probably should figure out some way to re-use these tests in bakana's downstream packages.

function isDataFrameWithSimpleColumns(x) {
    expect(x instanceof bioc.DataFrame).toBe(true);
    for (const cn of x.columnNames()) {
        let col = x.column(cn);
        expect(
            col instanceof Array || 
            (ArrayBuffer.isView(col) && !(col instanceof DataView)) ||
            col instanceof bioc.DataFrame // for back-compatibity purposes only in the SE reader... this probably shouldn't be allowed. 
        ).toBe(true);
    }
}

function isArrayOfUniqueNames(x) {
    expect(x.length).toBeGreaterThan(0);
    expect(x instanceof Array).toBe(true);
    for (const y of x) {
        expect(typeof y).toEqual("string") 
    }
    expect((new Set(x)).size).toEqual(x.length);
}

export async function checkDatasetGeneral(dataset) {
    expect(typeof dataset.constructor.format()).toEqual("string");
    expect(dataset.abbreviate().constructor).toBe(Object);
}

export async function checkDatasetSerialize(dataset) {
    let serialized = await dataset.serialize();
    expect(serialized.constructor).toBe(Object); 
    for (const fentry of serialized.files) {
        expect(typeof fentry.type).toEqual("string");
        expect(fentry.file instanceof bakana.SimpleFile).toEqual(true);
    }
    expect(serialized.options.constructor).toBe(Object); 

    let copy = await dataset.constructor.unserialize(serialized.files, serialized.options);
    expect(copy instanceof dataset.constructor).toBe(true);
    expect(copy.constructor.format()).toEqual(dataset.constructor.format());
    expect(copy.abbreviate()).toEqual(dataset.abbreviate());
    return copy;
}

export async function checkDatasetSummary(dataset) {
    let summ = await dataset.summary();
    if ("all_features" in summ) {
        expect("modality_features" in summ).toBe(false);
        isDataFrameWithSimpleColumns(summ.all_features);
        expect(summ.all_features.numberOfRows()).toBeGreaterThan(0);
    } else {
        expect("modality_features" in summ).toBe(true);
        for (const [mod, df] of Object.entries(summ.modality_features)) {
            isDataFrameWithSimpleColumns(df);
            expect(df.numberOfRows()).toBeGreaterThan(0);
        }
    }

    isDataFrameWithSimpleColumns(summ.cells);
    expect(summ.cells.numberOfRows()).toBeGreaterThan(0);

    if ("all_assay_names" in summ) {
        expect("modality_assay_names" in summ).toBe(false);
        isArrayOfUniqueNames(summ.all_assay_names);
    } else if ("modality_assay_names" in summ) {
        expect("all_assay_names" in summ).toBe(false);
        for (const [mod, nms] of Object.entries(summ.modality_assay_names)) {
            isArrayOfUniqueNames(nms);
        }
    }

    return summ;
}

function sameDataFrame(left, right) {
    expect(left.numberOfRows()).toEqual(right.numberOfRows());
    expect(left.columnNames()).toEqual(right.columnNames());
}

export function sameDatasetSummary(left, right) {
    expect(Object.keys(left)).toEqual(Object.keys(right));

    if ("all_features" in left) {
        sameDataFrame(left.all_features, right.all_features);
    } else {
        expect(Object.keys(left.modality_features)).toEqual(Object.keys(right.modality_features));
        for (const [mod, df] of Object.entries(left.modality_features)) {
            sameDataFrame(df, right.modality_features[mod]);
        }
    }

    sameDataFrame(left.cells, right.cells);

    if ("all_assay_names" in left) {
        expect(left.all_assay_names).toEqual(right.all_assay_names);
    } else {
        expect(left.modality_assay_names).toEqual(right.modality_assay_names);
    }
}

export async function checkDatasetLoad(dataset) {
    let loaded = await dataset.load();

    expect(loaded.matrix.numberOfColumns()).toBeGreaterThan(0);
    let available_modalities = loaded.matrix.available();
    expect(available_modalities.length).toBeGreaterThan(0);
    for (const mod of available_modalities) {
        expect(["RNA", "ADT", "CRISPR"].indexOf(mod)).toBeGreaterThanOrEqual(0);
        let mat = loaded.matrix.get(mod);
        expect(mat.numberOfRows()).toBeGreaterThan(0);
        expect(mat.isSparse()).toEqual(true); // we force all count matrices to be sparse.
    }

    isDataFrameWithSimpleColumns(loaded.cells);
    expect(loaded.cells.numberOfRows()).toEqual(loaded.matrix.numberOfColumns());

    expect(available_modalities).toEqual(Object.keys(loaded.features));
    expect(available_modalities).toEqual(Object.keys(loaded.primary_ids));
    for (const mod of available_modalities) {
        const df = loaded.features[mod];
        expect(df.numberOfRows()).toEqual(loaded.matrix.get(mod).numberOfRows());
        isDataFrameWithSimpleColumns(df);
        const pid = loaded.primary_ids[mod];
        if (pid !== null) {
            expect(df.numberOfRows()).toEqual(pid.length);
            expect(pid.every(y => typeof y === "string" || y == null)).toBe(true);
        }
    }

    let preview = await dataset.previewPrimaryIds();
    expect(preview).toEqual(loaded.primary_ids);
    return loaded;
}

export function sameDatasetLoad(left, right) {
    let available_modalities = left.matrix.available();
    expect(available_modalities).toEqual(right.matrix.available());
    for (const mod of available_modalities) {
        expect(left.matrix.get(mod).numberOfRows()).toEqual(right.matrix.get(mod).numberOfRows());
        expect(left.matrix.get(mod).numberOfColumns()).toEqual(right.matrix.get(mod).numberOfColumns());
        expect(left.matrix.get(mod).row(0)).toEqual(right.matrix.get(mod).row(0));
        expect(left.matrix.get(mod).column(0)).toEqual(right.matrix.get(mod).column(0));
    }

    expect(Object.keys(left.features)).toEqual(available_modalities);
    expect(Object.keys(right.features)).toEqual(available_modalities);
    for (const mod of available_modalities) {
        sameDataFrame(left.features[mod], right.features[mod]);
    }

    sameDataFrame(left.cells, right.cells);
    expect(left.primary_ids).toEqual(right.primary_ids);
}

export async function checkResultSummary(result) {
    let summ = await result.summary();
    if ("all_features" in summ) {
        expect("modality_features" in summ).toBe(false);
        isDataFrameWithSimpleColumns(summ.all_features);
        expect(summ.all_features.numberOfRows()).toBeGreaterThan(0);
    } else {
        expect("modality_features" in summ).toBe(true);
        for (const [mod, df] of Object.entries(summ.modality_features)) {
            isDataFrameWithSimpleColumns(df);
            expect(df.numberOfRows()).toBeGreaterThan(0);
        }
    }

    isDataFrameWithSimpleColumns(summ.cells);
    expect(summ.cells.numberOfRows()).toBeGreaterThan(0);

    if ("all_assay_names" in summ) {
        expect("modality_assay_names" in summ).toBe(false);
        isArrayOfUniqueNames(summ.all_assay_names);
    } else {
        expect("all_assay_names" in summ).toBe(false);
        for (const [mod, nms] of Object.entries(summ.modality_assay_names)) {
            isArrayOfUniqueNames(nms);
        }
    }

    if ("reduced_dimension_names" in summ && summ.length > 0) {
        isArrayOfUniqueNames(summ.reduced_dimension_names);
    }
    if ("other_metadata" in summ) {
        expect(summ.other_metadata.constructor).toBe(Object);
    }

    return summ;
}

export async function checkResultLoad(result) {
    let loaded = await result.load();

    expect(loaded.matrix.numberOfColumns()).toBeGreaterThan(0);
    let available_modalities = loaded.matrix.available();
    expect(available_modalities.length).toBeGreaterThan(0);
    for (const mod of available_modalities) {
        expect(loaded.matrix.get(mod).numberOfRows()).toBeGreaterThan(0);
    }

    isDataFrameWithSimpleColumns(loaded.cells);
    expect(loaded.cells.numberOfRows()).toEqual(loaded.matrix.numberOfColumns());

    expect(available_modalities).toEqual(Object.keys(loaded.features));
    for (const mod of available_modalities) {
        const df = loaded.features[mod];
        isDataFrameWithSimpleColumns(df);
        expect(df.numberOfRows()).toEqual(loaded.matrix.get(mod).numberOfRows());
    }

    if ("reduced_dimension_names" in loaded) {
        for (const [name, vals] of Object.entries(loaded)) {
            for (const dim of vals) {
                expect(dim instanceof Float64Array).toBe(true);
                expect(dim.length).toEqual(loaded.cells.numberOfRows());
            }
        }
    }
    if ("other_metadata" in loaded) {
        expect(loaded.other_metadata.constructor).toBe(Object);
    }

    return loaded;
}
