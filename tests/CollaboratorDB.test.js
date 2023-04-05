import * as bakana from "bakana";
import * as scran from "scran.js";
import * as remotes from "../src/index.js";
import * as utils from "./utils.js";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

let id = "dssc-test_basic-2023:my_first_sce@2023-01-19";
let cdb = new remotes.CollaboratordbDataset(id);

remotes.CollaboratordbDataset.setDownloadFun(utils.downloader);
remotes.CollaboratordbDataset.setGetFun(async url => new Response(await utils.downloader(url)));

test("CollaboratorDB abbreviation works as expected", async () => {
    let abbrev = cdb.abbreviate();
    expect(abbrev.id).toBe(id);
    expect(abbrev.options.crisprCountAssay).toEqual(0);
    expect(abbrev.options.adtExperiment).toBe("Antibody Capture");
    expect(abbrev.options.primaryRnaFeatureIdColumn).toBeNull();
    expect(cdb.constructor.format()).toBe("CollaboratorDB");
})

test("CollaboratorDB preflight works as expected", async () => {
    let pre = await cdb.summary();
    expect(pre.modality_assay_names[""]).toEqual(["counts"]);
    expect(pre.modality_assay_names.ERCC).toEqual(["counts"]);
    expect(pre.modality_features[""].numberOfRows()).toBeGreaterThan(0);
    expect(pre.modality_features.ERCC.numberOfRows()).toBeGreaterThan(0);
    expect(pre.cells.column("level1class").length).toBeGreaterThan(0);
})

test("CollaboratorDB loading works as expected", async () => {
    let details = await cdb.load();
    expect(details.features.RNA.numberOfRows()).toBeGreaterThan(0);
    expect(details.cells.numberOfRows()).toBeGreaterThan(0);
    expect(details.cells.hasColumn("level1class")).toBe(true);

    let mat = details.matrix.get("RNA");
    expect(mat.numberOfRows()).toEqual(details.features["RNA"].numberOfRows());
    expect(mat.numberOfColumns()).toEqual(details.cells.numberOfRows());
    expect(details.row_ids.RNA.length).toEqual(mat.numberOfRows());

    expect(details.features.RNA.rowNames().length).toBeGreaterThan(0); // check that the primary ID was correctly set.

    // Trying again with strings everywhere.
    let copy = new remotes.CollaboratordbDataset(id);
    copy.setOptions({
        rnaCountAssay: "counts",
        adtExperiment: "ERCC",
        crisprExperiment: 0
    });

    let details2 = await copy.load();
    expect(details2.matrix.get("ADT").numberOfRows()).toBeLessThan(details2.matrix.get("RNA").numberOfRows());
    expect(details2.matrix.get("CRISPR").numberOfRows()).toEqual(details2.matrix.get("CRISPR").numberOfRows());

    // Catch some errors!
    copy.setOptions({ rnaCountAssay: "FOO" });
    await expect(copy.load()).rejects.toThrow("'FOO' not found")
    copy.setOptions({ rnaCountAssay: 100 });
    await expect(copy.load()).rejects.toThrow("out of range")

    scran.free(details2.matrix);
    scran.free(details.matrix);
})

test("CollaboratorDB loading works as part of the wider bakana analysis", async () => {
    bakana.availableReaders["CollaboratorDB"] = remotes.CollaboratordbDataset;
    let files = { default: cdb };

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
        expect(dec.decode(saved[0])).toBe(id);
        expect(serialized.parameters).toEqual(bakana.retrieveParameters(state));

        let reloaded = bakana.unserializeDatasets(serialized.datasets, x => saved[Number(x) - 1]); 
        expect(reloaded.default instanceof remotes.CollaboratordbDataset);
    }

    // Freeing.
    await bakana.freeAnalysis(state);
})


