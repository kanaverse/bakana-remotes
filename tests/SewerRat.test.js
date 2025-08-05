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
const pseudoDownload = url => {
    const path = url.replace(/.*path=/, "");
    return utils.downloader(gypsumUrl + "/file/" + path);
}
remotes.SewerRatDataset.setDownloadFun(pseudoDownload);
remotes.SewerRatResult.setDownloadFun(pseudoDownload);

const target = "scRNAseq/zeisel-brain-2015/2023-12-14"
let target_manifest = null;

const pseudoCheck = async url => {
    if (target_manifest == null) {
        const raw_man = await utils.downloader(gypsumUrl + "/file/" + encodeURIComponent(target + "/..manifest"));
        const dec = new TextDecoder;
        target_manifest = JSON.parse(dec.decode(raw_man));
    }
    const full_path = decodeURIComponent(url.replace(/.*path=/, ""));
    const sub_path = full_path.slice(target.length + 1);
    return (sub_path in target_manifest);
}
remotes.SewerRatDataset.setCheckFun(pseudoCheck);
remotes.SewerRatResult.setCheckFun(pseudoCheck);

test("SewerRat abbreviation works as expected", async () => {
    const srdb = new remotes.SewerRatDataset(target, "https://sewerrat.com");

    let summ = await utils.checkDatasetSummary(srdb);
    expect(Object.keys(summ.modality_features)).toEqual(["gene", "repeat", "ERCC"]);

    srdb.setOptions({ rnaExperiment: "gene", adtExperiment: "ERCC" });
    let loaded = await utils.checkDatasetLoad(srdb);
    expect(loaded.matrix.available()).toEqual(["RNA", "ADT"]);

    let copy = await utils.checkDatasetSerialize(srdb);
    utils.sameDatasetSummary(summ, await copy.summary());
    utils.sameDatasetLoad(loaded, await copy.load());

    srdb.clear();
})

test("SewerRat result loading works as expected", async () => {
    const srdb = new remotes.SewerRatResult(target, "https://sewerrat.com");

    let summ = await utils.checkResultSummary(srdb);
    expect(Object.keys(summ.modality_assay_names)).toEqual(["gene", "repeat", "ERCC"]);
    
    let loaded = await utils.checkResultLoad(srdb);
    expect(Object.keys(summ.modality_assay_names)).toEqual(["gene", "repeat", "ERCC"]);
    await utils.checkResultSummary(srdb);
    await utils.checkResultLoad(srdb);
})
