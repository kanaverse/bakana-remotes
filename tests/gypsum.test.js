import * as bakana from "bakana";
import * as scran from "scran.js";
import * as remotes from "../src/index.js";
import * as utils from "./utils.js";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

let details = { project: "scRNAseq", asset: "zeisel-brain-2015", version: "2023-12-14", path: null };
remotes.GypsumDataset.setGetFun(utils.downloader);
remotes.GypsumResult.setGetFun(utils.downloader);

test("gypsum abbreviation works as expected", async () => {
    let gdb = new remotes.GypsumDataset(details.project, details.asset, details.version, details.path);
    let abbrev = gdb.abbreviate();
    expect(abbrev.id.project).toEqual(details.project);
    expect(abbrev.id.asset).toEqual(details.asset);
    expect(abbrev.options.crisprCountAssay).toEqual(0);
    expect(abbrev.options.adtExperiment).toBe("Antibody Capture");
    expect(abbrev.options.primaryRnaFeatureIdColumn).toBeNull();
    expect(gdb.constructor.format()).toBe("gypsum");
})

test("gypsum preflight works as expected", async () => {
    let gdb = new remotes.GypsumDataset(details.project, details.asset, details.version, details.path);
    let pre = await gdb.summary();
    expect(pre.modality_assay_names.gene).toEqual(["counts"]);
    expect(pre.modality_assay_names.ERCC).toEqual(["counts"]);
    expect(pre.modality_features.gene.numberOfRows()).toBeGreaterThan(0);
    expect(pre.modality_features.ERCC.numberOfRows()).toBeGreaterThan(0);
    expect(pre.cells.column("level1class").length).toBeGreaterThan(0);
})

test("gypsum loading works as expected", async () => {
    let gdb = new remotes.GypsumDataset(details.project, details.asset, details.version, details.path);
    let loaded = await gdb.load();
    expect(loaded.features.RNA.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.hasColumn("level1class")).toBe(true);

    let mat = loaded.matrix.get("RNA");
    expect(mat.numberOfRows()).toEqual(loaded.features["RNA"].numberOfRows());
    expect(mat.numberOfColumns()).toEqual(loaded.cells.numberOfRows());

    expect(loaded.features.RNA.rowNames().length).toBeGreaterThan(0); // check that the primary ID was correctly set.

    // Trying again with strings everywhere.
    let copy = new remotes.GypsumDataset(details.project, details.asset, details.version, details.path);
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

test("gypsum loading works as part of the wider bakana analysis", async () => {
    bakana.availableReaders["gypsum"] = remotes.GypsumDataset;
    let gdb = new remotes.GypsumDataset(details.project, details.asset, details.version, details.path);
    let files = { default: gdb };

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
        expect(JSON.parse(dec.decode(saved[0]))).toEqual(gdb.abbreviate().id);
        expect(serialized.parameters).toEqual(bakana.retrieveParameters(state));

        let reloaded = bakana.unserializeDatasets(serialized.datasets, x => saved[Number(x) - 1]); 
        expect(reloaded.default instanceof remotes.GypsumDataset);
    }

    // Freeing.
    await bakana.freeAnalysis(state);
})

test("gypsum result loading works as expected", async () => {
    let gdb = new remotes.GypsumResult(details.project, details.asset, details.version, details.path);
    let pre = await gdb.summary();
    expect(pre.modality_assay_names.gene).toEqual(["counts"]);
    expect(pre.modality_assay_names.ERCC).toEqual(["counts"]);
    expect(pre.modality_features.gene.numberOfRows()).toBeGreaterThan(0);
    expect(pre.modality_features.ERCC.numberOfRows()).toBeGreaterThan(0);
    expect(pre.cells.column("level1class").length).toBeGreaterThan(0);

    let loaded = await gdb.load();
    expect(loaded.features.gene.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.features.ERCC.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.numberOfRows()).toBeGreaterThan(0);
    expect(loaded.cells.hasColumn("level1class")).toBe(true);
})
