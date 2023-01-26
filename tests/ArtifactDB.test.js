import * as remotes from "../src/index.js";
import * as utils from "./utils.js";
import * as bakana from "bakana";

beforeAll(utils.initializeAll);
afterAll(async () => await bakana.terminate());

let test_url = "https://collaboratordb.aaron-lun.workers.dev";
let getter = async url => new Response(await utils.downloader(url));

test("Loading a CSV DataFrame works as expected", async () => {
    let options = { getFun: getter, downloadFun: utils.downloader };

    let df = await remotes.loadDataFrame(test_url, "dssc-test_basic-2023:my_first_df@2023-01-19", options);
    expect(df.columnNames()).toEqual(["X", "Y", "Z"]);
    expect(df.column("X") instanceof Float64Array).toBe(true);
    expect(df.numberOfRows()).toEqual(10);
    expect(df.rowNames()).toBeNull();

    // Works with row names.
    let df2 = await remotes.loadDataFrame(test_url, "dssc-test_basic-2023:my_first_sce/rowdata/simple.csv.gz@2023-01-19", options);
    expect(df2.column("featureType").length).toBe(df2.numberOfRows());
    expect(df2.rowNames().length).toBe(df2.numberOfRows());
})

test("Loading a ScranMatrix works as expected", async () => {
    let options = { getFun: getter, downloadFun: utils.downloader };

    // Loading from a sparse matrix.
    {
        let test_id = "dssc-test_basic-2023:my_first_sce/assay-1/matrix.h5@2023-01-19";

        let mat = await remotes.loadScranMatrix(test_url, test_id, options);
        expect(mat.matrix.numberOfRows()).toBeGreaterThan(0);
        expect(mat.matrix.numberOfColumns()).toBeGreaterThan(0);
        expect(mat.matrix.isSparse()).toBe(true);
        expect(mat.row_ids.length).toEqual(mat.matrix.numberOfRows());

        // Trying without any layering.
        let unlayered = await remotes.loadScranMatrix(test_url, test_id, { ...options, layered: false });
        expect(unlayered.matrix.numberOfRows()).toEqual(mat.matrix.numberOfRows());
        expect(unlayered.matrix.numberOfColumns()).toEqual(mat.matrix.numberOfColumns());
        expect(unlayered.row_ids.length).toEqual(unlayered.matrix.numberOfRows());

        let mcol = mat.matrix.column(0);
        let ucol = unlayered.matrix.column(0);
        expect(Array.from(mat.row_ids).map(i => ucol[i])).toEqual(Array.from(mcol));
    }

    // Loading a sparse matrix from a dense matrix.
    {
        let mat = await remotes.loadScranMatrix(test_url, "dssc-test_airway-2023:airway/assay-1/array.h5@2023-01-19", options);
        expect(mat.matrix.numberOfRows()).toBeGreaterThan(0);
        expect(mat.matrix.numberOfColumns()).toBeGreaterThan(0);
        expect(mat.matrix.isSparse()).toBe(true);
        expect(mat.row_ids.length).toEqual(mat.matrix.numberOfRows());
    }
})
