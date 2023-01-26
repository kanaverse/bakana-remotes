import * as bakana from "bakana";
import * as scran from "scran.js";
import * as remotes from "../src/index.js";
import * as utils from "./utils.js";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

let ehub = new remotes.ExperimentHubDataset("zeisel-brain");

test("ExperimentHub abbreviation works as expected", async () => {
    let abbrev = ehub.abbreviate();
    expect(abbrev.id).toBe("zeisel-brain");
    expect(abbrev.options.primaryRnaFeatureIdColumn).toEqual(0);
    expect(ehub.constructor.format()).toBe("ExperimentHub");
})

test("ExperimentHub preflight works as expected", async () => {
    let pre = await ehub.summary();
    expect(pre.modality_features.RNA.numberOfRows()).toBeGreaterThan(0);
    expect(pre.cells.column("level1class").length).toBeGreaterThan(0);
})

test("ExperimentHub loading works as expected", async () => {
    let details = await ehub.load();
    expect(details.features.RNA.numberOfRows()).toBeGreaterThan(0);
    expect(details.cells.numberOfRows()).toBeGreaterThan(0);
    expect(details.cells.hasColumn("level1class")).toBe(true);

    let mat = details.matrix.get("RNA");
    expect(mat.numberOfRows()).toEqual(details.features["RNA"].numberOfRows());
    expect(mat.numberOfColumns()).toEqual(details.cells.numberOfRows());

    scran.free(details.matrix);
})

test("ExperimentHub works as part of the wider bakana analysis", async () => {
    bakana.availableReaders["ExperimentHub"] = remotes.ExperimentHubDataset;
    let files = { default: ehub };

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
    expect(dec.decode(collected.collected[0])).toBe("zeisel-brain");

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

test("loading of the Segertolpe dataset works as expected", async () => {
    let test = new remotes.ExperimentHubDataset("segerstolpe-pancreas");
    let loaded = await test.load();

    expect(loaded.matrix.get("RNA").numberOfRows()).toEqual(loaded.features["RNA"].numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfRows()).toBeGreaterThan(0);

    expect(loaded.matrix.get("RNA").numberOfColumns()).toEqual(loaded.cells.numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfColumns()).toBeGreaterThan(0);

    scran.free(test.matrix);
})

test("loading of the Nestorowa dataset works as expected", async () => {
    let test = new remotes.ExperimentHubDataset("nestorowa-hsc");
    let loaded = await test.load();

    expect(loaded.matrix.get("RNA").numberOfRows()).toEqual(loaded.features["RNA"].numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfRows()).toBeGreaterThan(0);

    expect(loaded.matrix.get("RNA").numberOfColumns()).toEqual(loaded.cells.numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfColumns()).toBeGreaterThan(0);

    scran.free(test.matrix);
})

test("loading of the Aztekin dataset works as expected", async () => {
    let test = new remotes.ExperimentHubDataset("aztekin-tail");
    let loaded = await test.load();

    expect(loaded.matrix.get("RNA").numberOfRows()).toEqual(loaded.features["RNA"].numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfRows()).toBeGreaterThan(0);

    expect(loaded.matrix.get("RNA").numberOfColumns()).toEqual(loaded.cells.numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfColumns()).toBeGreaterThan(0);

    scran.free(test.matrix);
})

test("loading of the Wu dataset works as expected", async () => {
    let test = new remotes.ExperimentHubDataset("wu-kidney");
    let loaded = await test.load();

    expect(loaded.matrix.get("RNA").numberOfRows()).toEqual(loaded.features["RNA"].numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfRows()).toBeGreaterThan(0);

    expect(loaded.matrix.get("RNA").numberOfColumns()).toEqual(loaded.cells.numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfColumns()).toBeGreaterThan(0);

    scran.free(test.matrix);
})

test("loading of the Zilionis dataset works as expected", async () => {
    let test = new remotes.ExperimentHubDataset("zilionis-mouse-lung");
    let loaded = await test.load();

    expect(loaded.matrix.get("RNA").numberOfRows()).toEqual(loaded.features["RNA"].numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfRows()).toBeGreaterThan(0);

    expect(loaded.matrix.get("RNA").numberOfColumns()).toEqual(loaded.cells.numberOfRows());
    expect(loaded.matrix.get("RNA").numberOfColumns()).toBeGreaterThan(0);

    scran.free(test.matrix);
})

