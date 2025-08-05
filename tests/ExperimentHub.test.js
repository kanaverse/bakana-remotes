import * as bakana from "bakana";
import * as scran from "scran.js";
import * as remotes from "../src/index.js";
import * as utils from "./utils.js";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

remotes.ExperimentHubDataset.setDownloadFun(utils.downloader);

test("ExperimentHub reader works as expected", async () => {
    let ehub = new remotes.ExperimentHubDataset("zeisel-brain");

    let summ = await utils.checkDatasetSummary(ehub);
    expect(Object.keys(summ.modality_features)).toEqual(["RNA"]);

    let loaded = await utils.checkDatasetLoad(ehub);
    expect(loaded.matrix.available()).toEqual(["RNA"]);

    let copy = await utils.checkDatasetSerialize(ehub);
    utils.sameDatasetSummary(summ, await copy.summary());
    utils.sameDatasetLoad(loaded, await copy.load());

    ehub.clear();
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

