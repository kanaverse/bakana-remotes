import * as bakana from "bakana";
import * as scran from "scran.js";
import * as remotes from "../src/index.js";
import * as utils from "./utils.js";
import * as fs from "fs";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

// I can't be bothered to set up an actual SewerRat instance, so instead we'll
// just re-use the Gypsum data store to mock the getter and listing functions.
const gypsumUrl = "https://gypsum.artifactdb.com"
const pseudoGet = url => {
    const path = url.replace(/.*path=/, "");
    return utils.downloader(gypsumUrl + "/file/" + path);
}
remotes.SewerRatDataset.setGetFun(pseudoGet);
remotes.SewerRatResult.setGetFun(pseudoGet);

const pseudoList = async url => {
    const path = url.replace(/.*path=/, "");
    const res = await fetch(gypsumUrl + "/list?prefix=" + path + "%2F");
    if (!res.ok) {
        throw new Error("failed to list path prefix '" + path + "'");
    }
    const listing = await res.json();
    for (let [i, k] of listing.entries()) {
        if (k.endsWith("/")) {
            k = k.slice(0, k.length - 1);
        }
        const j = k.lastIndexOf("/");
        listing[i] = k.slice(j + 1, k.length);
    }
    return listing;
}
remotes.SewerRatDataset.setListFun(pseudoList);
remotes.SewerRatResult.setListFun(pseudoList);

const target = "scRNAseq/zeisel-brain-2015/2023-12-14"
const srdb = new remotes.SewerRatDataset(target, "https://sewerrat.com");

test("SewerRat abbreviation works as expected", async () => {
    let abbrev = srdb.abbreviate();
    expect(abbrev.id.path).toEqual(target);
    expect(abbrev.options.crisprCountAssay).toEqual(0);
    expect(abbrev.options.adtExperiment).toBe("Antibody Capture");
    expect(abbrev.options.primaryRnaFeatureIdColumn).toBeNull();
    expect(srdb.constructor.format()).toBe("SewerRat");
})

test("SewerRat preflight works as expected", async () => {
    let pre = await srdb.summary();
    expect(pre.modality_assay_names.gene).toEqual(["counts"]);
    expect(pre.modality_assay_names.ERCC).toEqual(["counts"]);
    expect(pre.modality_features.gene.numberOfRows()).toBeGreaterThan(0);
    expect(pre.modality_features.ERCC.numberOfRows()).toBeGreaterThan(0);
    expect(pre.cells.column("level1class").length).toBeGreaterThan(0);
})

test("SewerRat loading works as expected", async () => {
    let loaded = await srdb.load();
    expect(loaded.features.RNA.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.hasColumn("level1class")).toBe(true);

    let mat = loaded.matrix.get("RNA");
    expect(mat.numberOfRows()).toEqual(loaded.features["RNA"].numberOfRows());
    expect(mat.numberOfColumns()).toEqual(loaded.cells.numberOfRows());

    expect(loaded.features.RNA.rowNames().length).toBeGreaterThan(0); // check that the primary ID was correctly set.

    // Trying again with strings everywhere.
    const copy = new remotes.SewerRatDataset(target, "https://sewerrat.com");
    copy.setOptions({
        rnaCountAssay: "counts",
        adtExperiment: "ERCC",
        crisprExperiment: 0
    });

    let loaded2 = await copy.load();
    expect(loaded2.matrix.get("ADT").numberOfRows()).toBeLessThan(loaded2.matrix.get("RNA").numberOfRows());
    expect(loaded2.matrix.get("CRISPR").numberOfRows()).toEqual(loaded2.matrix.get("CRISPR").numberOfRows());

    // Catch some errors!
    copy.setOptions({ rnaCountAssay: "FOO" });
    await expect(copy.load()).rejects.toThrow("'FOO' not found")
    copy.setOptions({ rnaCountAssay: 100 });
    await expect(copy.load()).rejects.toThrow("out of range")

    scran.free(loaded2.matrix);
    scran.free(loaded.matrix);
})

test("SewerRat loading works as part of the wider bakana analysis", async () => {
    bakana.availableReaders["SewerRat"] = remotes.SewerRatDataset;
    let files = { default: srdb };

    let state = await bakana.createAnalysis();
    let params = utils.baseParams();
    await bakana.runAnalysis(state, files, params);

    let nr = state.inputs.fetchCountMatrix().get("RNA").numberOfRows();
    expect(nr).toBeGreaterThan(0);
    expect(state.inputs.fetchFeatureAnnotations()["RNA"].numberOfRows()).toBe(nr);
    expect(state.inputs.fetchCellAnnotations().numberOfRows()).toBeGreaterThan(0);

    expect(state.rna_quality_control.changed).toBe(true);
    expect(state.rna_pca.changed).toBe(true);
    expect(state.feature_selection.changed).toBe(true);
    expect(state.cell_labelling.changed).toBe(true);
    expect(state.marker_detection.changed).toBe(true);

    // Serialization works as expected.
    {
        let saved = [];
        let saver = (n, k, f) => {
            saved.push(f.content());
            return String(saved.length);
        };

        let serialized = await bakana.serializeConfiguration(state, saver);
        const dec = new TextDecoder;
        expect(JSON.parse(dec.decode(saved[0]))).toEqual(srdb.abbreviate().id);
        expect(serialized.parameters).toEqual(bakana.retrieveParameters(state));

        let reloaded = bakana.unserializeDatasets(serialized.datasets, x => saved[Number(x) - 1]); 
        expect(reloaded.default instanceof remotes.SewerRatDataset);
    }

    // Freeing.
    await bakana.freeAnalysis(state);
})

test("SewerRat result loading works as expected", async () => {
    const srdb = new remotes.SewerRatResult(target, "https://sewerrat.com");
    let pre = await srdb.summary();
    expect(pre.modality_assay_names.gene).toEqual(["counts"]);
    expect(pre.modality_assay_names.ERCC).toEqual(["counts"]);
    expect(pre.modality_features.gene.numberOfRows()).toBeGreaterThan(0);
    expect(pre.modality_features.ERCC.numberOfRows()).toBeGreaterThan(0);
    expect(pre.cells.column("level1class").length).toBeGreaterThan(0);

    let loaded = await srdb.load();
    expect(loaded.features.gene.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.features.ERCC.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.hasColumn("level1class")).toBe(true);
})
