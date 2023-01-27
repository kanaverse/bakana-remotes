import * as bakana from "bakana";
import * as scran from "scran.js";
import * as remotes from "../src/index.js";
import * as utils from "./utils.js";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

let id = "dssc-test_basic-2023:my_first_sce@2023-01-19";
let cdb = new remotes.CollaboratorDBDataset(id);

remotes.CollaboratorDBDataset.setDownloadFun(utils.downloader);
remotes.CollaboratorDBDataset.setGetFun(async url => new Response(await utils.downloader(url)));

test("CollaboratorDB abbreviation works as expected", async () => {
    let abbrev = cdb.abbreviate();
    expect(abbrev.id).toBe(id);
    expect(abbrev.options.countAssayCrisprName).toBeNull();
    expect(abbrev.options.experimentAdtName).toBeNull();
    expect(abbrev.options.primaryRnaFeatureIdColumn).toEqual(0);
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

    scran.free(details.matrix);
})

test("CollaboratorDB loading works as part of the wider bakana analysis", async () => {
    bakana.availableReaders["CollaboratorDB"] = remotes.CollaboratorDBDataset;
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
    const path = "TEST_state_ExperimentHub.h5";
    let collected = await bakana.saveAnalysis(state, path);
    utils.validateState(path);
    expect(collected.collected.length).toBe(1);

    const dec = new TextDecoder;
    expect(dec.decode(collected.collected[0])).toBe(id);

    let offsets = utils.mockOffsets(collected.collected);
    let reloaded = await bakana.loadAnalysis(
        path, 
        (offset, size) => offsets[offset]
    );

    let new_params = bakana.retrieveParameters(reloaded);
    expect(new_params.rna_quality_control instanceof Object).toBe(true);
    expect(new_params.rna_pca instanceof Object).toBe(true);

    // Freeing.
    await bakana.freeAnalysis(state);
    await bakana.freeAnalysis(reloaded);
})


