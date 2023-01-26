import * as remotes from "../src/index.js";
import * as utils from "./utils.js";

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
