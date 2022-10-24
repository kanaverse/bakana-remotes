import * as bakana from "bakana";
import * as scran from "scran.js";
import * as ehub from "../src/ExperimentHub.js"
import * as utils from "./utils.js";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

let args = { id: "zeisel-brain" };

test("ExperimentHub abbreviation works as expected", async () => {
    let abbrev = ehub.abbreviate(args);
    expect(abbrev.format).toBe("ExperimentHub");
    expect(abbrev.id).toBe(args.id);
})

test("ExperimentHub preflight works as expected", async () => {
    let pre = await ehub.preflight(args);
    expect(pre.genes.RNA.id.length).toBeGreaterThan(0);
    expect(pre.annotations.level1class.type).toBe("categorical");
    expect(pre.annotations.level1class.values.length).toBeGreaterThan(0);
})

test("ExperimentHub loading works as expected", async () => {
    let reader = new ehub.Reader(args);
    let details = await reader.load();
    expect(details.genes.RNA.id.length).toBeGreaterThan(0);
    expect(details.annotations.level1class.length).toBeGreaterThan(0);

    let mat = details.matrix.get("RNA");
    expect(mat.numberOfRows()).toEqual(details.genes.RNA.id.length);
    expect(mat.numberOfColumns()).toEqual(details.annotations.level1class.length);
})

test("ExperimentHub works as part of the wider bakana analysis", async () => {
    let contents = {};
    let finished = (step, res) => {
        contents[step] = res;
    };

    bakana.availableReaders["ExperimentHub"] = ehub;
    let files = { 
        default: {
            format: "ExperimentHub",
            id: "zeisel-brain"
        }
    };

    let state = await bakana.createAnalysis();
    let params = utils.baseParams();
    let res = await bakana.runAnalysis(state, files, params, { finishFun: finished });

    expect(contents.quality_control instanceof Object).toBe(true);
    expect(contents.pca instanceof Object).toBe(true);
    expect(contents.feature_selection instanceof Object).toBe(true);
    expect(contents.cell_labelling instanceof Object).toBe(true);
    expect(contents.marker_detection instanceof Object).toBe(true);

    // Serialization works as expected.
    const path = "TEST_state_ExperimentHub.h5";
    let collected = await bakana.saveAnalysis(state, path);
    utils.validateState(path);
    expect(collected.collected.length).toBe(1);
    expect(typeof(collected.collected[0])).toBe("string");

    let offsets = utils.mockOffsets(collected.collected);
    let reloaded = await bakana.loadAnalysis(
        path, 
        (offset, size) => offsets[offset]
    );

    let new_params = bakana.retrieveParameters(reloaded);
    expect(new_params.quality_control instanceof Object).toBe(true);
    expect(new_params.pca instanceof Object).toBe(true);

    // Freeing.
    await bakana.freeAnalysis(state);
    await bakana.freeAnalysis(reloaded);
})
