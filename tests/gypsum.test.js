import * as bakana from "bakana";
import * as scran from "scran.js";
import * as remotes from "../src/index.js";
import * as utils from "./utils.js";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

let details = { project: "scRNAseq", asset: "zeisel-brain-2015", version: "2023-12-14", path: null };
remotes.GypsumDataset.setDownloadFun(utils.downloader);
remotes.GypsumResult.setDownloadFun(utils.downloader);

test("gypsum reader works as expected", async () => {
    let gdb = new remotes.GypsumDataset(details.project, details.asset, details.version, details.path);

    let summ = await utils.checkDatasetSummary(gdb);
    expect(Object.keys(summ.modality_features)).toEqual(["gene", "repeat", "ERCC"]);
    expect(summ.modality_assay_names["gene"]).toEqual(["counts"]);

    gdb.setOptions({ rnaExperiment: "gene", adtExperiment: "ERCC" });
    let loaded = await utils.checkDatasetLoad(gdb);
    expect(loaded.matrix.available()).toEqual(["RNA", "ADT"]);

    let copy = await utils.checkDatasetSerialize(gdb);
    utils.sameDatasetSummary(summ, await copy.summary());
    utils.sameDatasetLoad(loaded, await copy.load());

    gdb.clear();
})

test("gypsum reader follows links", async () => {
    // 2024-04-18 links back to an older version.
    let gdb = new remotes.GypsumDataset("scRNAseq", "nestorowa-hsc-2016", "2024-04-18", null);

    let summ = await utils.checkDatasetSummary(gdb);
    expect(Object.keys(summ.modality_features)).toEqual(["endogenous", "ERCC", "FACS"]);
    expect(summ.modality_assay_names["endogenous"]).toEqual(["counts"]);

    gdb.clear();
})

test("gypsum reader respects a non-NULL path", async () => {
    let gdb = new remotes.GypsumDataset("scRNAseq", "wu-kidney-2019", "2023-12-20", "disease");

    let summ = await utils.checkDatasetSummary(gdb);
    expect(Object.keys(summ.modality_features)).toEqual([""]);
    expect(summ.modality_assay_names[""]).toEqual(["counts"]);

    gdb.clear();
})

test("gypsum result loading works as expected", async () => {
    let gdb = new remotes.GypsumResult(details.project, details.asset, details.version, details.path);

    let summ = await utils.checkResultSummary(gdb);
    expect(Object.keys(summ.modality_assay_names)).toEqual(["gene", "repeat", "ERCC"]);
    
    let loaded = await utils.checkResultLoad(gdb);
    expect(Object.keys(summ.modality_assay_names)).toEqual(["gene", "repeat", "ERCC"]);
})
