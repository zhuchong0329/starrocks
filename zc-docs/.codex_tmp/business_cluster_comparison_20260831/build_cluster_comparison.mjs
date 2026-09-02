import fs from "node:fs/promises";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const oldPath = "/Users/zhuchong/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/CCD6B86731662BF7521135C8689ABC49/Caches/Files/2026-08/ab1ae1d03c025890b1cf3fe07c546e11/国内老集群business库各表数据量统计（双副本）.csv";
const newPath = "/Users/zhuchong/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/CCD6B86731662BF7521135C8689ABC49/Caches/Files/2026-08/a41f7420a1e649fab3a8a9199690c590/国内新集群business库各表数据量统计（双副本）.csv";
const outputDir = "/Users/zhuchong/Documents/code/starrocks-main/outputs/business_cluster_comparison_20260831";
const previewDir = "/Users/zhuchong/Documents/code/starrocks-main/.codex_tmp/business_cluster_comparison_20260831/previews";
const outputPath = `${outputDir}/新老集群business库数据量分析与对比.xlsx`;

const TIB = 1024 ** 4;
const GIB = 1024 ** 3;

async function loadCsv(path, sheetName) {
  const csvText = await fs.readFile(path, "utf8");
  const imported = await Workbook.fromCSV(csvText, { sheetName });
  const values = imported.worksheets.getItem(sheetName).getUsedRange().values;
  const headers = values[0].map((v) => String(v));
  const idx = Object.fromEntries(headers.map((h, i) => [h, i]));
  const required = ["table", "total_rows", "raw_total_bytes", "disk_total_bytes", "raw_bpr", "disk_bpr", "compression_ratio", "disk_size"];
  for (const field of required) if (!(field in idx)) throw new Error(`Missing field ${field} in ${path}`);
  return values.slice(1).filter((r) => r[idx.table] !== null && r[idx.table] !== "").map((r) => ({
    table: String(r[idx.table]),
    totalRows: Number(r[idx.total_rows]),
    rawBytes: Number(r[idx.raw_total_bytes]),
    diskBytes: Number(r[idx.disk_total_bytes]),
    rawBpr: Number(r[idx.raw_bpr]),
    diskBpr: Number(r[idx.disk_bpr]),
    compressionRatio: Number(r[idx.compression_ratio]),
    diskSize: String(r[idx.disk_size]),
  }));
}

function stats(data) {
  const sorted = [...data].sort((a, b) => b.diskBytes - a.diskBytes);
  const totalRows = data.reduce((s, d) => s + d.totalRows, 0);
  const totalRaw = data.reduce((s, d) => s + d.rawBytes, 0);
  const totalDisk = data.reduce((s, d) => s + d.diskBytes, 0);
  const topShare = (count) => sorted.slice(0, count).reduce((s, d) => s + d.diskBytes, 0) / totalDisk;
  const medianGib = [...data].map((d) => d.diskBytes / GIB).sort((a, b) => a - b);
  const middle = medianGib.length % 2 ? medianGib[(medianGib.length - 1) / 2] : (medianGib[medianGib.length / 2 - 1] + medianGib[medianGib.length / 2]) / 2;
  return { sorted, totalRows, totalRaw, totalDisk, topShare, medianGib: middle, weightedCompression: totalRaw / totalDisk };
}

function capLevel(diskBytes) {
  if (diskBytes === null || diskBytes === undefined) return "";
  const gib = diskBytes / GIB;
  if (gib >= 1024) return "超大表";
  if (gib >= 100) return "大表";
  if (gib >= 10) return "中表";
  return "小表";
}

const oldData = await loadCsv(oldPath, "老集群源数据");
const newData = await loadCsv(newPath, "新集群源数据");
const oldStats = stats(oldData);
const newStats = stats(newData);
const oldMap = new Map(oldData.map((d) => [d.table, d]));
const newMap = new Map(newData.map((d) => [d.table, d]));
const unionNames = [...new Set([...oldMap.keys(), ...newMap.keys()])];

const comparisons = unionNames.map((table) => {
  const old = oldMap.get(table) ?? null;
  const fresh = newMap.get(table) ?? null;
  const status = old && fresh ? "两边都有" : fresh ? "仅新集群" : "仅老集群";
  const diskDelta = (fresh?.diskBytes ?? 0) - (old?.diskBytes ?? 0);
  const rowDelta = (fresh?.totalRows ?? 0) - (old?.totalRows ?? 0);
  return { table, old, fresh, status, diskDelta, rowDelta };
}).sort((a, b) => Math.abs(b.diskDelta) - Math.abs(a.diskDelta));

const decreases = comparisons.filter((d) => d.diskDelta < 0).sort((a, b) => a.diskDelta - b.diskDelta).slice(0, 10);
const increases = comparisons.filter((d) => d.diskDelta > 0).sort((a, b) => b.diskDelta - a.diskDelta).slice(0, 10);
const topCompareNames = [...unionNames].sort((a, b) => {
  const av = Math.max(oldMap.get(a)?.diskBytes ?? 0, newMap.get(a)?.diskBytes ?? 0);
  const bv = Math.max(oldMap.get(b)?.diskBytes ?? 0, newMap.get(b)?.diskBytes ?? 0);
  return bv - av;
}).slice(0, 8);
const compareRowByName = new Map(comparisons.map((d, i) => [d.table, 5 + i]));

const workbook = Workbook.create();
const compareSummary = workbook.worksheets.add("新老对比");
const compareDetail = workbook.worksheets.add("表级对比");
const newSummary = workbook.worksheets.add("新集群汇总");
const newLists = workbook.worksheets.add("新集群大小表");
const newDetail = workbook.worksheets.add("新集群明细");

const navy = "#17365D";
const blue = "#2F75B5";
const oldGray = "#7F8C8D";
const lightBlue = "#D9EAF7";
const paleBlue = "#EEF5FB";
const paleOrange = "#FCE4D6";
const paleGreen = "#E2F0D9";
const paleRed = "#FDE9E7";
const paleGray = "#F2F4F7";
const gray = "#667085";
const border = "#CFD6DE";
const white = "#FFFFFF";

function titleBand(sheet, range, value) {
  sheet.mergeCells(range);
  sheet.getRange(range.split(":")[0]).values = [[value]];
  sheet.getRange(range).format = {
    fill: navy,
    font: { bold: true, color: white, size: 18 },
    verticalAlignment: "center",
    horizontalAlignment: "left",
    borders: { preset: "outside", style: "thin", color: navy },
  };
  sheet.getRange(range).format.rowHeight = 34;
}

function sectionHeader(sheet, range, value) {
  sheet.mergeCells(range);
  sheet.getRange(range.split(":")[0]).values = [[value]];
  sheet.getRange(range).format = {
    fill: lightBlue,
    font: { bold: true, color: navy, size: 12 },
    verticalAlignment: "center",
    borders: { preset: "outside", style: "thin", color: border },
  };
  sheet.getRange(range).format.rowHeight = 24;
}

function styleHeader(sheet, range) {
  sheet.getRange(range).format = {
    fill: navy,
    font: { bold: true, color: white },
    horizontalAlignment: "center",
    verticalAlignment: "center",
    wrapText: true,
    borders: { preset: "outside", style: "thin", color: navy },
  };
}

function addCard(sheet, labelRange, valueRange, label, formula, fill) {
  sheet.mergeCells(labelRange);
  sheet.mergeCells(valueRange);
  sheet.getRange(labelRange.split(":")[0]).values = [[label]];
  sheet.getRange(valueRange.split(":")[0]).formulas = [[formula]];
  const block = `${labelRange.split(":")[0]}:${valueRange.split(":")[1]}`;
  sheet.getRange(block).format = {
    fill,
    borders: { preset: "outside", style: "thin", color: border },
    verticalAlignment: "center",
    horizontalAlignment: "center",
  };
  sheet.getRange(labelRange).format.font = { bold: true, color: gray, size: 10 };
  sheet.getRange(valueRange).format.font = { bold: true, color: navy, size: 20 };
}

function addCapacityConditionalFormats(range) {
  range.conditionalFormats.add("containsText", { text: "大表", format: { fill: paleOrange, font: { color: "#9C5700", bold: true } } });
  range.conditionalFormats.add("containsText", { text: "超大表", format: { fill: "#F8CBAD", font: { color: "#9C0006", bold: true } } });
  range.conditionalFormats.add("containsText", { text: "中表", format: { fill: paleBlue, font: { color: navy } } });
  range.conditionalFormats.add("containsText", { text: "小表", format: { fill: paleGreen, font: { color: "#375623" } } });
}

// 新集群明细页。
newDetail.showGridLines = false;
titleBand(newDetail, "A1:P1", "新集群 business 数据库各表规模明细（双副本）");
newDetail.mergeCells("A2:P2");
newDetail.getRange("A2").values = [["按 disk_total_bytes 从大到小排序；容量与行数分层由“新集群汇总”页阈值驱动。"]];
newDetail.getRange("A2:P2").format = { fill: paleBlue, font: { color: gray, italic: true }, wrapText: true, verticalAlignment: "center" };
newDetail.getRange("A2:P2").format.rowHeight = 28;
newDetail.getRange("A4:P4").values = [["容量排名", "表名", "容量级别", "行数级别", "总行数", "行数（亿）", "原始字节数", "磁盘字节数（双副本）", "磁盘容量（TiB）", "容量占比", "累计容量占比", "原始数据量（TiB）", "原始B/行", "磁盘B/行", "压缩比", "原容量文本"]];
styleHeader(newDetail, "A4:P4");
newDetail.getRange("A4:P4").format.rowHeight = 34;

const newStart = 5;
const newEnd = newStart + newStats.sorted.length - 1;
newDetail.getRange(`A${newStart}:P${newEnd}`).values = newStats.sorted.map((d) => [null, d.table, null, null, d.totalRows, null, d.rawBytes, d.diskBytes, null, null, null, null, d.rawBpr, d.diskBpr, d.compressionRatio, d.diskSize]);
newDetail.getRange(`A${newStart}`).formulas = [["=ROW()-4"]];
newDetail.getRange(`A${newStart}:A${newEnd}`).fillDown();
newDetail.getRange(`C${newStart}`).formulas = [[`=IF(I${newStart}*1024>='新集群汇总'!$B$14,"超大表",IF(I${newStart}*1024>='新集群汇总'!$B$15,"大表",IF(I${newStart}*1024>='新集群汇总'!$B$16,"中表","小表")))`]];
newDetail.getRange(`C${newStart}:C${newEnd}`).fillDown();
newDetail.getRange(`D${newStart}`).formulas = [[`=IF(E${newStart}>='新集群汇总'!$H$14,"超高行数",IF(E${newStart}>='新集群汇总'!$H$15,"高行数",IF(E${newStart}>='新集群汇总'!$H$16,"中行数","低行数")))`]];
newDetail.getRange(`D${newStart}:D${newEnd}`).fillDown();
newDetail.getRange(`F${newStart}`).formulas = [[`=E${newStart}/100000000`]];
newDetail.getRange(`F${newStart}:F${newEnd}`).fillDown();
newDetail.getRange(`I${newStart}`).formulas = [[`=H${newStart}/1099511627776`]];
newDetail.getRange(`I${newStart}:I${newEnd}`).fillDown();
newDetail.getRange(`J${newStart}`).formulas = [[`=H${newStart}/SUM($H$${newStart}:$H$${newEnd})`]];
newDetail.getRange(`J${newStart}:J${newEnd}`).fillDown();
newDetail.getRange(`K${newStart}`).formulas = [[`=SUM($J$${newStart}:J${newStart})`]];
newDetail.getRange(`K${newStart}:K${newEnd}`).fillDown();
newDetail.getRange(`L${newStart}`).formulas = [[`=G${newStart}/1099511627776`]];
newDetail.getRange(`L${newStart}:L${newEnd}`).fillDown();
newDetail.getRange(`A${newStart}:P${newEnd}`).format = { font: { size: 10 }, verticalAlignment: "center", borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } } };
newDetail.getRange(`A${newStart}:A${newEnd}`).format.horizontalAlignment = "center";
newDetail.getRange(`C${newStart}:D${newEnd}`).format.horizontalAlignment = "center";
newDetail.getRange(`E${newStart}:O${newEnd}`).format.horizontalAlignment = "right";
newDetail.getRange(`E${newStart}:E${newEnd}`).format.numberFormat = "#,##0";
newDetail.getRange(`F${newStart}:F${newEnd}`).format.numberFormat = "#,##0.00";
newDetail.getRange(`G${newStart}:H${newEnd}`).format.numberFormat = "#,##0";
newDetail.getRange(`I${newStart}:I${newEnd}`).format.numberFormat = "#,##0.000";
newDetail.getRange(`J${newStart}:K${newEnd}`).format.numberFormat = "0.00%";
newDetail.getRange(`L${newStart}:L${newEnd}`).format.numberFormat = "#,##0.000";
newDetail.getRange(`M${newStart}:O${newEnd}`).format.numberFormat = "#,##0.00";
addCapacityConditionalFormats(newDetail.getRange(`C${newStart}:C${newEnd}`));
newDetail.getRange(`J${newStart}:J${newEnd}`).conditionalFormats.add("dataBar", { color: blue, gradient: true });
const newDetailTable = newDetail.tables.add(`A4:P${newEnd}`, true, "NewClusterDetail");
newDetailTable.style = "TableStyleMedium2";
newDetailTable.showFilterButton = true;
newDetail.freezePanes.freezeRows(4);
newDetail.freezePanes.freezeColumns(2);
const newDetailWidths = [10, 35, 11, 12, 16, 12, 21, 22, 16, 12, 15, 17, 13, 13, 11, 15];
for (let c = 0; c < newDetailWidths.length; c++) newDetail.getCell(3, c).format.columnWidth = newDetailWidths[c];

// 新集群汇总页。
newSummary.showGridLines = false;
titleBand(newSummary, "A1:M1", "新集群 business 数据库表数据量分析摘要");
newSummary.mergeCells("A2:M2");
newSummary.getRange("A2").values = [["统计口径：disk_total_bytes 为双副本磁盘总量；容量分层与老集群上一版保持一致。来源：国内新集群business库各表数据量统计（双副本）.csv"]];
newSummary.getRange("A2:M2").format = { fill: paleBlue, font: { color: gray, italic: true }, wrapText: true, verticalAlignment: "center" };
newSummary.getRange("A2:M2").format.rowHeight = 30;
addCard(newSummary, "A4:C4", "A5:C6", "表数量", `=COUNTA('新集群明细'!$B$${newStart}:$B$${newEnd})`, paleBlue);
addCard(newSummary, "D4:F4", "D5:F6", "总行数（万亿）", `=SUM('新集群明细'!$E$${newStart}:$E$${newEnd})/1000000000000`, paleBlue);
addCard(newSummary, "G4:I4", "G5:I6", "双副本磁盘总量（TiB）", `=SUM('新集群明细'!$I$${newStart}:$I$${newEnd})`, paleOrange);
addCard(newSummary, "J4:M4", "J5:M6", "Top 5 容量占比", `=SUM('新集群明细'!$J$${newStart}:$J$${newStart + 4})`, paleOrange);
addCard(newSummary, "A8:C8", "A9:C10", "整体加权压缩比", `=SUM('新集群明细'!$G$${newStart}:$G$${newEnd})/SUM('新集群明细'!$H$${newStart}:$H$${newEnd})`, paleGreen);
addCard(newSummary, "D8:F8", "D9:F10", "单副本估算（TiB）", `=SUM('新集群明细'!$I$${newStart}:$I$${newEnd})/2`, paleGreen);
addCard(newSummary, "G8:I8", "G9:I10", "单表磁盘中位数（GiB）", `=MEDIAN('新集群明细'!$I$${newStart}:$I$${newEnd})*1024`, paleBlue);
addCard(newSummary, "J8:M8", "J9:M10", "最大表容量占比", `=MAX('新集群明细'!$J$${newStart}:$J$${newEnd})`, paleOrange);
newSummary.getRange("D5:F6").format.numberFormat = "0.00";
newSummary.getRange("G5:I6").format.numberFormat = "#,##0.00";
newSummary.getRange("J5:M6").format.numberFormat = "0.0%";
newSummary.getRange("A9:C10").format.numberFormat = "0.00";
newSummary.getRange("D9:F10").format.numberFormat = "#,##0.00";
newSummary.getRange("G9:I10").format.numberFormat = "#,##0.00";
newSummary.getRange("J9:M10").format.numberFormat = "0.0%";

sectionHeader(newSummary, "A12:E12", "容量分层（主口径）");
newSummary.getRange("A13:E13").values = [["类别", "下限（GiB）", "表数", "磁盘容量（TiB）", "容量占比"]];
newSummary.getRange("A14:B17").values = [["超大表", 1024], ["大表", 100], ["中表", 10], ["小表", 0]];
for (let row = 14; row <= 17; row++) newSummary.getRange(`C${row}:E${row}`).formulas = [[`=COUNTIF('新集群明细'!$C$${newStart}:$C$${newEnd},A${row})`, `=SUMIF('新集群明细'!$C$${newStart}:$C$${newEnd},A${row},'新集群明细'!$I$${newStart}:$I$${newEnd})`, `=D${row}/SUM($D$14:$D$17)`]];
sectionHeader(newSummary, "G12:K12", "行数分层（辅助口径）");
newSummary.getRange("G13:K13").values = [["类别", "下限（行）", "表数", "行数（万亿）", "行数占比"]];
newSummary.getRange("G14:H17").values = [["超高行数", 10000000000], ["高行数", 1000000000], ["中行数", 100000000], ["低行数", 0]];
for (let row = 14; row <= 17; row++) newSummary.getRange(`I${row}:K${row}`).formulas = [[`=COUNTIF('新集群明细'!$D$${newStart}:$D$${newEnd},G${row})`, `=SUMIF('新集群明细'!$D$${newStart}:$D$${newEnd},G${row},'新集群明细'!$E$${newStart}:$E$${newEnd})/1000000000000`, `=J${row}/SUM($J$14:$J$17)`]];
for (const range of ["A13:E13", "G13:K13"]) styleHeader(newSummary, range);
for (const range of ["A14:E17", "G14:K17"]) newSummary.getRange(range).format = { borders: { insideHorizontal: { style: "thin", color: border } }, verticalAlignment: "center" };
newSummary.getRange("B14:B17").format.numberFormat = "#,##0";
newSummary.getRange("D14:D17").format.numberFormat = "#,##0.000";
newSummary.getRange("E14:E17").format.numberFormat = "0.00%";
newSummary.getRange("H14:H17").format.numberFormat = "#,##0";
newSummary.getRange("J14:J17").format.numberFormat = "0.000";
newSummary.getRange("K14:K17").format.numberFormat = "0.00%";
newSummary.getRange("A14:A17").format.font = { bold: true };
newSummary.getRange("G14:G17").format.font = { bold: true };

sectionHeader(newSummary, "A20:F20", "Top 10 大表（按双副本磁盘容量）");
newSummary.getRange("A22:F22").values = [["排名", "表名", "磁盘容量（TiB）", "容量占比", "行数（亿）", "压缩比"]];
styleHeader(newSummary, "A22:F22");
const newTop10 = [];
for (let i = 0; i < Math.min(10, newStats.sorted.length); i++) {
  const r = newStart + i;
  newTop10.push([`='新集群明细'!A${r}`, `='新集群明细'!B${r}`, `='新集群明细'!I${r}`, `='新集群明细'!J${r}`, `='新集群明细'!F${r}`, `='新集群明细'!O${r}`]);
}
newSummary.getRange(`A23:F${22 + newTop10.length}`).formulas = newTop10;
newSummary.getRange(`A23:F${22 + newTop10.length}`).format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
newSummary.getRange(`C23:C${22 + newTop10.length}`).format.numberFormat = "#,##0.00";
newSummary.getRange(`D23:D${22 + newTop10.length}`).format.numberFormat = "0.0%";
newSummary.getRange(`E23:E${22 + newTop10.length}`).format.numberFormat = "#,##0.00";
newSummary.getRange(`F23:F${22 + newTop10.length}`).format.numberFormat = "0.00";
newSummary.getRange(`C23:C${22 + newTop10.length}`).conditionalFormats.add("dataBar", { color: blue, gradient: true });
const newChart = newSummary.charts.add("bar", newSummary.getRange(`B22:C${22 + newTop10.length}`));
newChart.title = "新集群 Top 10 表磁盘容量（TiB，双副本）";
newChart.hasLegend = false;
newChart.setPosition("H20", "M36");
if (newChart.series.items.length > 0) newChart.series.items[0].fill = blue;

const newSmall = newStats.sorted.filter((d) => d.diskBytes / GIB < 10);
const newSmallDisk = newSmall.reduce((s, d) => s + d.diskBytes, 0);
sectionHeader(newSummary, "A35:M35", "核心结论与建议");
const newInsights = [
  `1. 新集群共 ${newData.length} 张表、${(newStats.totalRows / 1e12).toFixed(2)} 万亿行，双副本磁盘 ${ (newStats.totalDisk / TIB).toFixed(2)} TiB。`,
  `2. 容量仍然高度集中：最大表占 ${(newStats.topShare(1) * 100).toFixed(1)}%，Top 5 占 ${(newStats.topShare(5) * 100).toFixed(1)}%，Top 10 占 ${(newStats.topShare(10) * 100).toFixed(1)}%。`,
  `3. network_security_log_local 与 http_log_local 合计 ${((newStats.sorted[0].diskBytes + newStats.sorted[1].diskBytes) / TIB).toFixed(2)} TiB，占 ${(((newStats.sorted[0].diskBytes + newStats.sorted[1].diskBytes) / newStats.totalDisk) * 100).toFixed(1)}%。`,
  `4. dns_log_local 行数最多（${(newMap.get("dns_log_local").totalRows / 1e8).toFixed(2)} 亿行），但容量为 ${(newMap.get("dns_log_local").diskBytes / TIB).toFixed(2)} TiB；仍需同时看行数与行宽。`,
  `5. ${newSmall.length} 张小表合计仅 ${(newSmallDisk / TIB).toFixed(3)} TiB（${(newSmallDisk / GIB).toFixed(2)} GiB），容量治理应优先聚焦头部表。`,
];
for (let i = 0; i < newInsights.length; i++) {
  const row = 36 + i;
  newSummary.mergeCells(`A${row}:M${row}`);
  newSummary.getRange(`A${row}`).values = [[newInsights[i]]];
  newSummary.getRange(`A${row}:M${row}`).format = { fill: i % 2 === 0 ? paleGray : white, font: { color: "#344054" }, wrapText: true, verticalAlignment: "center" };
  newSummary.getRange(`A${row}:M${row}`).format.rowHeight = 30;
}
newSummary.freezePanes.freezeRows(2);
const newSummaryWidths = [11, 17, 16, 14, 14, 12, 12, 17, 13, 14, 13, 13, 13];
for (let c = 0; c < newSummaryWidths.length; c++) newSummary.getCell(12, c).format.columnWidth = newSummaryWidths[c];
newSummary.getRange("B23").format.columnWidth = 36;

// 新集群大小表清单。
const newLarge = newStats.sorted.filter((d) => d.diskBytes / GIB >= 100);
newLists.showGridLines = false;
titleBand(newLists, "A1:G1", `新集群大表清单（≥100 GiB，共 ${newLarge.length} 张）`);
titleBand(newLists, "I1:O1", `新集群小表清单（<10 GiB，共 ${newSmall.length} 张）`);
newLists.mergeCells("A2:G2");
newLists.getRange("A2").values = [["包含“超大表”和“大表”，按双副本磁盘容量降序。"]];
newLists.mergeCells("I2:O2");
newLists.getRange("I2").values = [[`小表合计 ${(newSmallDisk / TIB).toFixed(3)} TiB，仅占 ${(newSmallDisk / newStats.totalDisk * 100).toFixed(3)}%。`]];
newLists.getRange("A2:G2").format = { fill: paleOrange, font: { color: gray, italic: true }, wrapText: true };
newLists.getRange("I2:O2").format = { fill: paleGreen, font: { color: gray, italic: true }, wrapText: true };
const listHeaders = [["容量级别", "容量排名", "表名", "总行数", "磁盘容量（TiB）", "容量占比", "压缩比"]];
newLists.getRange("A4:G4").values = listHeaders;
newLists.getRange("I4:O4").values = listHeaders;
styleHeader(newLists, "A4:G4");
styleHeader(newLists, "I4:O4");
const largeFormulas = newLarge.map((d) => {
  const r = newStart + newStats.sorted.findIndex((x) => x.table === d.table);
  return [`='新集群明细'!C${r}`, `='新集群明细'!A${r}`, `='新集群明细'!B${r}`, `='新集群明细'!E${r}`, `='新集群明细'!I${r}`, `='新集群明细'!J${r}`, `='新集群明细'!O${r}`];
});
const smallFormulas = newSmall.map((d) => {
  const r = newStart + newStats.sorted.findIndex((x) => x.table === d.table);
  return [`='新集群明细'!C${r}`, `='新集群明细'!A${r}`, `='新集群明细'!B${r}`, `='新集群明细'!E${r}`, `='新集群明细'!I${r}`, `='新集群明细'!J${r}`, `='新集群明细'!O${r}`];
});
newLists.getRange(`A5:G${4 + largeFormulas.length}`).formulas = largeFormulas;
newLists.getRange(`I5:O${4 + smallFormulas.length}`).formulas = smallFormulas;
for (const range of [`A5:G${4 + largeFormulas.length}`, `I5:O${4 + smallFormulas.length}`]) newLists.getRange(range).format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
newLists.getRange(`D5:D${4 + largeFormulas.length}`).format.numberFormat = "#,##0";
newLists.getRange(`L5:L${4 + smallFormulas.length}`).format.numberFormat = "#,##0";
newLists.getRange(`E5:E${4 + largeFormulas.length}`).format.numberFormat = "#,##0.000";
newLists.getRange(`M5:M${4 + smallFormulas.length}`).format.numberFormat = "#,##0.000";
newLists.getRange(`F5:F${4 + largeFormulas.length}`).format.numberFormat = "0.00%";
newLists.getRange(`N5:N${4 + smallFormulas.length}`).format.numberFormat = "0.00%";
newLists.getRange(`G5:G${4 + largeFormulas.length}`).format.numberFormat = "0.00";
newLists.getRange(`O5:O${4 + smallFormulas.length}`).format.numberFormat = "0.00";
addCapacityConditionalFormats(newLists.getRange(`A5:A${4 + largeFormulas.length}`));
addCapacityConditionalFormats(newLists.getRange(`I5:I${4 + smallFormulas.length}`));
const largeTable = newLists.tables.add(`A4:G${4 + largeFormulas.length}`, true, "NewLargeTables");
largeTable.style = "TableStyleMedium9";
const smallTable = newLists.tables.add(`I4:O${4 + smallFormulas.length}`, true, "NewSmallTables");
smallTable.style = "TableStyleMedium4";
newLists.freezePanes.freezeRows(4);
const listWidths = { A: 11, B: 10, C: 35, D: 16, E: 17, F: 13, G: 11, H: 3, I: 11, J: 10, K: 35, L: 16, M: 17, N: 13, O: 11 };
for (const [col, width] of Object.entries(listWidths)) newLists.getRange(`${col}4`).format.columnWidth = width;

// 表级新老对比页，保留原始字节值并用公式计算差异。
compareDetail.showGridLines = false;
titleBand(compareDetail, "A1:V1", "新老集群 business 表级差异明细");
compareDetail.mergeCells("A2:V2");
compareDetail.getRange("A2").values = [["按磁盘容量绝对差异排序；仅存在于一侧的表单独标记，变化率仅对“两边都有”的表计算。"]];
compareDetail.getRange("A2:V2").format = { fill: paleBlue, font: { color: gray, italic: true }, wrapText: true, verticalAlignment: "center" };
compareDetail.getRange("A4:V4").values = [["差异排名", "表名", "存在状态", "老容量级别", "新容量级别", "老总行数", "新总行数", "行数差", "行数变化率", "老磁盘字节", "新磁盘字节", "老磁盘TiB", "新磁盘TiB", "磁盘差TiB", "磁盘变化率", "老原始字节", "新原始字节", "老压缩比", "新压缩比", "压缩比差", "老磁盘B/行", "新磁盘B/行"]];
styleHeader(compareDetail, "A4:V4");
compareDetail.getRange("A4:V4").format.rowHeight = 38;
const compareStart = 5;
const compareEnd = compareStart + comparisons.length - 1;
compareDetail.getRange(`A${compareStart}:V${compareEnd}`).values = comparisons.map((d, i) => [
  i + 1, d.table, d.status, null, null,
  d.old?.totalRows ?? null, d.fresh?.totalRows ?? null, null, null,
  d.old?.diskBytes ?? null, d.fresh?.diskBytes ?? null, null, null, null, null,
  d.old?.rawBytes ?? null, d.fresh?.rawBytes ?? null,
  d.old?.compressionRatio ?? null, d.fresh?.compressionRatio ?? null, null,
  d.old?.diskBpr ?? null, d.fresh?.diskBpr ?? null,
]);
compareDetail.getRange(`D${compareStart}`).formulas = [[`=IF(L${compareStart}="","",IF(L${compareStart}*1024>='新集群汇总'!$B$14,"超大表",IF(L${compareStart}*1024>='新集群汇总'!$B$15,"大表",IF(L${compareStart}*1024>='新集群汇总'!$B$16,"中表","小表"))))`]];
compareDetail.getRange(`D${compareStart}:D${compareEnd}`).fillDown();
compareDetail.getRange(`E${compareStart}`).formulas = [[`=IF(M${compareStart}="","",IF(M${compareStart}*1024>='新集群汇总'!$B$14,"超大表",IF(M${compareStart}*1024>='新集群汇总'!$B$15,"大表",IF(M${compareStart}*1024>='新集群汇总'!$B$16,"中表","小表"))))`]];
compareDetail.getRange(`E${compareStart}:E${compareEnd}`).fillDown();
compareDetail.getRange(`H${compareStart}`).formulas = [[`=IF(C${compareStart}="仅新集群",G${compareStart},IF(C${compareStart}="仅老集群",-F${compareStart},G${compareStart}-F${compareStart}))`]];
compareDetail.getRange(`H${compareStart}:H${compareEnd}`).fillDown();
compareDetail.getRange(`I${compareStart}`).formulas = [[`=IF(C${compareStart}="两边都有",IF(F${compareStart}=0,"",G${compareStart}/F${compareStart}-1),"")`]];
compareDetail.getRange(`I${compareStart}:I${compareEnd}`).fillDown();
compareDetail.getRange(`L${compareStart}`).formulas = [[`=IF(J${compareStart}="","",J${compareStart}/1099511627776)`]];
compareDetail.getRange(`L${compareStart}:L${compareEnd}`).fillDown();
compareDetail.getRange(`M${compareStart}`).formulas = [[`=IF(K${compareStart}="","",K${compareStart}/1099511627776)`]];
compareDetail.getRange(`M${compareStart}:M${compareEnd}`).fillDown();
compareDetail.getRange(`N${compareStart}`).formulas = [[`=IF(C${compareStart}="仅新集群",M${compareStart},IF(C${compareStart}="仅老集群",-L${compareStart},M${compareStart}-L${compareStart}))`]];
compareDetail.getRange(`N${compareStart}:N${compareEnd}`).fillDown();
compareDetail.getRange(`O${compareStart}`).formulas = [[`=IF(C${compareStart}="两边都有",IF(L${compareStart}=0,"",M${compareStart}/L${compareStart}-1),"")`]];
compareDetail.getRange(`O${compareStart}:O${compareEnd}`).fillDown();
compareDetail.getRange(`T${compareStart}`).formulas = [[`=IF(C${compareStart}="两边都有",S${compareStart}-R${compareStart},"")`]];
compareDetail.getRange(`T${compareStart}:T${compareEnd}`).fillDown();
compareDetail.getRange(`A${compareStart}:V${compareEnd}`).format = { font: { size: 10 }, verticalAlignment: "center", borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } } };
compareDetail.getRange(`A${compareStart}:A${compareEnd}`).format.horizontalAlignment = "center";
compareDetail.getRange(`C${compareStart}:E${compareEnd}`).format.horizontalAlignment = "center";
compareDetail.getRange(`F${compareStart}:H${compareEnd}`).format.numberFormat = "#,##0";
compareDetail.getRange(`I${compareStart}:I${compareEnd}`).format.numberFormat = "0.0%";
compareDetail.getRange(`J${compareStart}:K${compareEnd}`).format.numberFormat = "#,##0";
compareDetail.getRange(`L${compareStart}:N${compareEnd}`).format.numberFormat = "#,##0.000";
compareDetail.getRange(`O${compareStart}:O${compareEnd}`).format.numberFormat = "0.0%";
compareDetail.getRange(`P${compareStart}:Q${compareEnd}`).format.numberFormat = "#,##0";
compareDetail.getRange(`R${compareStart}:V${compareEnd}`).format.numberFormat = "#,##0.00";
addCapacityConditionalFormats(compareDetail.getRange(`D${compareStart}:E${compareEnd}`));
compareDetail.getRange(`C${compareStart}:C${compareEnd}`).conditionalFormats.add("containsText", { text: "仅新集群", format: { fill: paleGreen, font: { color: "#375623", bold: true } } });
compareDetail.getRange(`C${compareStart}:C${compareEnd}`).conditionalFormats.add("containsText", { text: "仅老集群", format: { fill: paleGray, font: { color: gray, bold: true } } });
compareDetail.getRange(`N${compareStart}:N${compareEnd}`).conditionalFormats.add("cellIs", { operator: "greaterThan", formula: 0, format: { fill: paleRed, font: { color: "#9C0006" } } });
compareDetail.getRange(`N${compareStart}:N${compareEnd}`).conditionalFormats.add("cellIs", { operator: "lessThan", formula: 0, format: { fill: paleGreen, font: { color: "#375623" } } });
const comparisonTable = compareDetail.tables.add(`A4:V${compareEnd}`, true, "ClusterTableComparison");
comparisonTable.style = "TableStyleMedium2";
comparisonTable.showFilterButton = true;
compareDetail.freezePanes.freezeRows(4);
compareDetail.freezePanes.freezeColumns(2);
const compareWidths = [10, 35, 12, 11, 11, 16, 16, 16, 13, 21, 21, 14, 14, 14, 13, 21, 21, 12, 12, 12, 14, 14];
for (let c = 0; c < compareWidths.length; c++) compareDetail.getCell(3, c).format.columnWidth = compareWidths[c];

// 新老对比总览页。
compareSummary.showGridLines = false;
titleBand(compareSummary, "A1:N1", "新老集群 business 数据量整体对比");
compareSummary.mergeCells("A2:N2");
compareSummary.getRange("A2").values = [["两份文件均为双副本容量快照；本页是横向对比，不据此推断迁移完成度或统计时间先后。缺失表按“仅新/仅老”单独展示。"]];
compareSummary.getRange("A2:N2").format = { fill: paleBlue, font: { color: gray, italic: true }, wrapText: true, verticalAlignment: "center" };
compareSummary.getRange("A2:N2").format.rowHeight = 30;
sectionHeader(compareSummary, "A4:E4", "总体指标对比");
compareSummary.getRange("A5:E5").values = [["指标", "老集群", "新集群", "新-老", "变化率"]];
styleHeader(compareSummary, "A5:E5");
const metricLabels = [["表数量"], ["总行数（万亿）"], ["双副本磁盘（TiB）"], ["单副本估算（TiB）"], ["原始数据（TiB）"], ["加权压缩比"], ["单表中位数（GiB）"], ["最大表容量占比"]];
compareSummary.getRange("A6:A13").values = metricLabels;
const metricFormulas = [
  [`=COUNT('表级对比'!$F$${compareStart}:$F$${compareEnd})`, `=COUNT('表级对比'!$G$${compareStart}:$G$${compareEnd})`],
  [`=SUM('表级对比'!$F$${compareStart}:$F$${compareEnd})/1000000000000`, `=SUM('表级对比'!$G$${compareStart}:$G$${compareEnd})/1000000000000`],
  [`=SUM('表级对比'!$L$${compareStart}:$L$${compareEnd})`, `=SUM('表级对比'!$M$${compareStart}:$M$${compareEnd})`],
  [`=B8/2`, `=C8/2`],
  [`=SUM('表级对比'!$P$${compareStart}:$P$${compareEnd})/1099511627776`, `=SUM('表级对比'!$Q$${compareStart}:$Q$${compareEnd})/1099511627776`],
  [`=SUM('表级对比'!$P$${compareStart}:$P$${compareEnd})/SUM('表级对比'!$J$${compareStart}:$J$${compareEnd})`, `=SUM('表级对比'!$Q$${compareStart}:$Q$${compareEnd})/SUM('表级对比'!$K$${compareStart}:$K$${compareEnd})`],
  [`=MEDIAN('表级对比'!$L$${compareStart}:$L$${compareEnd})*1024`, `=MEDIAN('表级对比'!$M$${compareStart}:$M$${compareEnd})*1024`],
  [`=MAX('表级对比'!$L$${compareStart}:$L$${compareEnd})/SUM('表级对比'!$L$${compareStart}:$L$${compareEnd})`, `=MAX('表级对比'!$M$${compareStart}:$M$${compareEnd})/SUM('表级对比'!$M$${compareStart}:$M$${compareEnd})`],
];
for (let i = 0; i < metricFormulas.length; i++) {
  const row = 6 + i;
  compareSummary.getRange(`B${row}:C${row}`).formulas = [[metricFormulas[i][0], metricFormulas[i][1]]];
  compareSummary.getRange(`D${row}:E${row}`).formulas = [[`=C${row}-B${row}`, `=IF(B${row}=0,"",D${row}/B${row})`]];
}
compareSummary.getRange("A6:E13").format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
compareSummary.getRange("A6:A13").format.font = { bold: true };
compareSummary.getRange("B6:D6").format.numberFormat = "0";
for (const row of [7, 8, 9, 10, 12]) compareSummary.getRange(`B${row}:D${row}`).format.numberFormat = "#,##0.00";
compareSummary.getRange("B11:D11").format.numberFormat = "0.00";
compareSummary.getRange("B13:D13").format.numberFormat = "0.0%";
compareSummary.getRange("E6:E13").format.numberFormat = "0.0%";
compareSummary.getRange("D6:D13").conditionalFormats.add("cellIs", { operator: "greaterThan", formula: 0, format: { fill: paleRed, font: { color: "#9C0006" } } });
compareSummary.getRange("D6:D13").conditionalFormats.add("cellIs", { operator: "lessThan", formula: 0, format: { fill: paleGreen, font: { color: "#375623" } } });

// 图表数据引用表级对比，保持可审计。
compareSummary.getRange("H18:K18").values = [["排名", "表名", "老集群TiB", "新集群TiB"]];
styleHeader(compareSummary, "H18:K18");
const topCompareFormulas = topCompareNames.map((name, i) => {
  const r = compareRowByName.get(name);
  return [i + 1, `='表级对比'!B${r}`, `='表级对比'!L${r}`, `='表级对比'!M${r}`];
});
compareSummary.getRange(`H19:K${18 + topCompareFormulas.length}`).values = topCompareFormulas.map((r) => [r[0], null, null, null]);
compareSummary.getRange(`I19:K${18 + topCompareFormulas.length}`).formulas = topCompareFormulas.map((r) => [r[1], r[2], r[3]]);
compareSummary.getRange(`J19:K${18 + topCompareFormulas.length}`).format.numberFormat = "#,##0.00";
const compareChart = compareSummary.charts.add("bar", compareSummary.getRange(`I18:K${18 + topCompareFormulas.length}`));
compareChart.title = "头部表双副本容量：老集群 vs 新集群（TiB）";
compareChart.hasLegend = true;
compareChart.setPosition("G4", "N16");
if (compareChart.series.items.length > 0) compareChart.series.items[0].fill = oldGray;
if (compareChart.series.items.length > 1) compareChart.series.items[1].fill = blue;

sectionHeader(compareSummary, "A17:G17", "容量级别对比");
compareSummary.getRange("A18:G18").values = [["类别", "下限GiB", "老表数", "新表数", "老容量TiB", "新容量TiB", "容量差TiB"]];
styleHeader(compareSummary, "A18:G18");
compareSummary.getRange("A18:G18").format.rowHeight = 34;
compareSummary.getRange("A19:B22").values = [["超大表", 1024], ["大表", 100], ["中表", 10], ["小表", 0]];
for (let row = 19; row <= 22; row++) compareSummary.getRange(`C${row}:G${row}`).formulas = [[
  `=COUNTIF('表级对比'!$D$${compareStart}:$D$${compareEnd},A${row})`,
  `=COUNTIF('表级对比'!$E$${compareStart}:$E$${compareEnd},A${row})`,
  `=SUMIF('表级对比'!$D$${compareStart}:$D$${compareEnd},A${row},'表级对比'!$L$${compareStart}:$L$${compareEnd})`,
  `=SUMIF('表级对比'!$E$${compareStart}:$E$${compareEnd},A${row},'表级对比'!$M$${compareStart}:$M$${compareEnd})`,
  `=F${row}-E${row}`,
]];
compareSummary.getRange("A19:G22").format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
compareSummary.getRange("B19:B22").format.numberFormat = "#,##0";
compareSummary.getRange("C19:D22").format.numberFormat = "0";
compareSummary.getRange("E19:G22").format.numberFormat = "#,##0.000";

sectionHeader(compareSummary, "A25:E25", "表存在状态");
compareSummary.getRange("A26:E26").values = [["状态", "表数", "老容量TiB", "新容量TiB", "净差TiB"]];
styleHeader(compareSummary, "A26:E26");
compareSummary.getRange("A27:A29").values = [["两边都有"], ["仅新集群"], ["仅老集群"]];
for (let row = 27; row <= 29; row++) compareSummary.getRange(`B${row}:E${row}`).formulas = [[
  `=COUNTIF('表级对比'!$C$${compareStart}:$C$${compareEnd},A${row})`,
  `=SUMIF('表级对比'!$C$${compareStart}:$C$${compareEnd},A${row},'表级对比'!$L$${compareStart}:$L$${compareEnd})`,
  `=SUMIF('表级对比'!$C$${compareStart}:$C$${compareEnd},A${row},'表级对比'!$M$${compareStart}:$M$${compareEnd})`,
  `=D${row}-C${row}`,
]];
compareSummary.getRange("A27:E29").format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
compareSummary.getRange("C27:E29").format.numberFormat = "#,##0.000";

sectionHeader(compareSummary, "A32:F32", "容量下降最多的表");
sectionHeader(compareSummary, "H32:M32", "容量上升最多的表");
const changeHeaders = [["排名", "表名", "状态", "老TiB", "新TiB", "差值TiB"]];
compareSummary.getRange("A33:F33").values = changeHeaders;
compareSummary.getRange("H33:M33").values = changeHeaders;
styleHeader(compareSummary, "A33:F33");
styleHeader(compareSummary, "H33:M33");
function changeFormulas(items, firstCol) {
  const matrix = items.map((item, i) => {
    const r = compareRowByName.get(item.table);
    return [i + 1, `='表级对比'!B${r}`, `='表级对比'!C${r}`, `='表级对比'!L${r}`, `='表级对比'!M${r}`, `='表级对比'!N${r}`];
  });
  const startColCode = firstCol === "A" ? "A" : "H";
  const endColCode = firstCol === "A" ? "F" : "M";
  const textStart = firstCol === "A" ? "B" : "I";
  const numericStart = firstCol === "A" ? "D" : "K";
  const numericEnd = firstCol === "A" ? "F" : "M";
  compareSummary.getRange(`${startColCode}34:${endColCode}${33 + matrix.length}`).values = matrix.map((r) => [r[0], null, null, null, null, null]);
  compareSummary.getRange(`${textStart}34:${endColCode}${33 + matrix.length}`).formulas = matrix.map((r) => r.slice(1));
  compareSummary.getRange(`${numericStart}34:${numericEnd}${33 + matrix.length}`).format.numberFormat = "#,##0.000";
  compareSummary.getRange(`${startColCode}34:${endColCode}${33 + matrix.length}`).format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
}
changeFormulas(decreases, "A");
changeFormulas(increases, "H");

sectionHeader(compareSummary, "A46:N46", "对比结论");
const deltaDisk = newStats.totalDisk - oldStats.totalDisk;
const deltaRows = newStats.totalRows - oldStats.totalRows;
const deltaRaw = newStats.totalRaw - oldStats.totalRaw;
const commonCount = comparisons.filter((d) => d.status === "两边都有").length;
const newOnlyCount = comparisons.filter((d) => d.status === "仅新集群").length;
const oldOnlyCount = comparisons.filter((d) => d.status === "仅老集群").length;
const comparisonInsights = [
  `1. 新集群双副本磁盘为 ${(newStats.totalDisk / TIB).toFixed(2)} TiB，较老集群减少 ${Math.abs(deltaDisk / TIB).toFixed(2)} TiB（${(deltaDisk / oldStats.totalDisk * 100).toFixed(1)}%）。`,
  `2. 新集群总行数较老集群减少 ${Math.abs(deltaRows / 1e8).toFixed(2)} 亿行（${(deltaRows / oldStats.totalRows * 100).toFixed(1)}%），原始数据量减少 ${Math.abs(deltaRaw / TIB).toFixed(2)} TiB（${(deltaRaw / oldStats.totalRaw * 100).toFixed(1)}%）。`,
  `3. 整体压缩比从 ${oldStats.weightedCompression.toFixed(2)} 降至 ${newStats.weightedCompression.toFixed(2)}；新集群磁盘量降幅小于原始数据量降幅，建议复核压缩与行宽变化。`,
  `4. 容量下降主要由 network_security_log_local（${(comparisons.find((d) => d.table === "network_security_log_local").diskDelta / TIB).toFixed(2)} TiB）和 http_log_local（${(comparisons.find((d) => d.table === "http_log_local").diskDelta / TIB).toFixed(2)} TiB）贡献。`,
  `5. ${commonCount} 张表两边都有，${newOnlyCount} 张仅新集群、${oldOnlyCount} 张仅老集群；该差异应结合采集范围和迁移清单复核。`,
];
for (let i = 0; i < comparisonInsights.length; i++) {
  const row = 47 + i;
  compareSummary.mergeCells(`A${row}:N${row}`);
  compareSummary.getRange(`A${row}`).values = [[comparisonInsights[i]]];
  compareSummary.getRange(`A${row}:N${row}`).format = { fill: i % 2 === 0 ? paleGray : white, font: { color: "#344054" }, wrapText: true, verticalAlignment: "center" };
  compareSummary.getRange(`A${row}:N${row}`).format.rowHeight = 30;
}
compareSummary.freezePanes.freezeRows(2);
const compareSummaryWidths = [18, 18, 15, 15, 15, 14, 14, 9, 34, 15, 15, 15, 15, 15];
for (let c = 0; c < compareSummaryWidths.length; c++) compareSummary.getCell(4, c).format.columnWidth = compareSummaryWidths[c];

// Verification and render pass for all sheets.
const compactChecks = [];
compactChecks.push((await workbook.inspect({ kind: "table", range: "新老对比!A4:E13", include: "values,formulas", tableMaxRows: 12, tableMaxCols: 5, maxChars: 5000 })).ndjson);
compactChecks.push((await workbook.inspect({ kind: "table", range: "新集群汇总!A4:K17", include: "values,formulas", tableMaxRows: 16, tableMaxCols: 11, maxChars: 6000 })).ndjson);
compactChecks.push((await workbook.inspect({ kind: "table", range: `表级对比!A4:V10`, include: "values,formulas", tableMaxRows: 7, tableMaxCols: 22, maxChars: 6000 })).ndjson);
compactChecks.push((await workbook.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A", options: { useRegex: true, maxResults: 300 }, summary: "final formula error scan" })).ndjson);
console.log(compactChecks.join("\n"));

await fs.mkdir(previewDir, { recursive: true });
for (const [sheetName, range, filename] of [
  ["新老对比", "A1:N51", "comparison_summary.png"],
  ["表级对比", `A1:V${compareEnd}`, "comparison_detail.png"],
  ["新集群汇总", "A1:M40", "new_summary.png"],
  ["新集群大小表", `A1:O${Math.max(4 + newLarge.length, 4 + newSmall.length)}`, "new_lists.png"],
  ["新集群明细", `A1:P${newEnd}`, "new_detail.png"],
]) {
  const preview = await workbook.render({ sheetName, range, scale: 1, format: "png" });
  await fs.writeFile(`${previewDir}/${filename}`, new Uint8Array(await preview.arrayBuffer()));
}

await fs.mkdir(outputDir, { recursive: true });
const xlsx = await SpreadsheetFile.exportXlsx(workbook);
await xlsx.save(outputPath);
await fs.rm(`${outputPath}.inspect.ndjson`, { force: true });

console.log(JSON.stringify({
  outputPath,
  old: { tables: oldData.length, rows: oldStats.totalRows, diskTiB: oldStats.totalDisk / TIB, rawTiB: oldStats.totalRaw / TIB, compression: oldStats.weightedCompression },
  fresh: { tables: newData.length, rows: newStats.totalRows, diskTiB: newStats.totalDisk / TIB, rawTiB: newStats.totalRaw / TIB, compression: newStats.weightedCompression, top1Share: newStats.topShare(1), top5Share: newStats.topShare(5), top10Share: newStats.topShare(10), largeCount: newLarge.length, smallCount: newSmall.length, smallDiskTiB: newSmallDisk / TIB },
  comparison: { union: comparisons.length, common: commonCount, newOnly: newOnlyCount, oldOnly: oldOnlyCount, diskDeltaTiB: deltaDisk / TIB, rowDelta: deltaRows, rawDeltaTiB: deltaRaw / TIB },
}, null, 2));
