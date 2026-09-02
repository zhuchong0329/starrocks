import fs from "node:fs/promises";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const sourcePath = "/Users/zhuchong/Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles/CCD6B86731662BF7521135C8689ABC49/Caches/Files/2026-08/ab1ae1d03c025890b1cf3fe07c546e11/国内老集群business库各表数据量统计（双副本）.csv";
const outputDir = "/Users/zhuchong/Documents/code/starrocks-main/zc-docs/outputs/business_table_analysis_20260831";
const previewDir = "/Users/zhuchong/Documents/code/starrocks-main/zc-docs/.codex_tmp/business_table_analysis_20260831/previews";
const outputPath = `${outputDir}/business库表数据量分析.xlsx`;

const csvText = await fs.readFile(sourcePath, "utf8");
const imported = await Workbook.fromCSV(csvText, { sheetName: "源数据" });
const importedSheet = imported.worksheets.getItem("源数据");
const rows = importedSheet.getUsedRange().values;
const headers = rows[0].map((v) => String(v));
const index = Object.fromEntries(headers.map((h, i) => [h, i]));
const n = (v) => Number(v);

const data = rows.slice(1).filter((r) => r[index.table] !== null && r[index.table] !== "").map((r) => ({
  table: String(r[index.table]),
  totalRows: n(r[index.total_rows]),
  rawBytes: n(r[index.raw_total_bytes]),
  diskBytes: n(r[index.disk_total_bytes]),
  rawBpr: n(r[index.raw_bpr]),
  diskBpr: n(r[index.disk_bpr]),
  compressionRatio: n(r[index.compression_ratio]),
  diskSize: String(r[index.disk_size]),
})).sort((a, b) => b.diskBytes - a.diskBytes);

const TIB = 1024 ** 4;
const GIB = 1024 ** 3;
const totalDisk = data.reduce((s, d) => s + d.diskBytes, 0);
const totalRaw = data.reduce((s, d) => s + d.rawBytes, 0);
const totalRows = data.reduce((s, d) => s + d.totalRows, 0);
const topShare = (count) => data.slice(0, count).reduce((s, d) => s + d.diskBytes, 0) / totalDisk;
const smallTables = data.filter((d) => d.diskBytes / GIB < 10);
const largeTables = data.filter((d) => d.diskBytes / GIB >= 100);
const smallDisk = smallTables.reduce((s, d) => s + d.diskBytes, 0);

const workbook = Workbook.create();
const summary = workbook.worksheets.add("汇总");
const detail = workbook.worksheets.add("明细");
const lists = workbook.worksheets.add("大小表清单");

const navy = "#17365D";
const blue = "#2F75B5";
const lightBlue = "#D9EAF7";
const paleBlue = "#EEF5FB";
const orange = "#F4B183";
const paleOrange = "#FCE4D6";
const green = "#70AD47";
const paleGreen = "#E2F0D9";
const gray = "#667085";
const paleGray = "#F2F4F7";
const border = "#CFD6DE";
const white = "#FFFFFF";

function titleBand(sheet, range, value) {
  sheet.mergeCells(range);
  const cell = sheet.getRange(range.split(":")[0]);
  cell.values = [[value]];
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
  const cell = sheet.getRange(range.split(":")[0]);
  cell.values = [[value]];
  sheet.getRange(range).format = {
    fill: lightBlue,
    font: { bold: true, color: navy, size: 12 },
    verticalAlignment: "center",
    borders: { preset: "outside", style: "thin", color: border },
  };
  sheet.getRange(range).format.rowHeight = 24;
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

// 明细页：原始值 + 公式驱动的分层、单位换算和占比。
detail.showGridLines = false;
titleBand(detail, "A1:P1", "business 数据库各表规模明细（双副本）");
detail.mergeCells("A2:P2");
detail.getRange("A2").values = [["按 disk_total_bytes 从大到小排序；容量级别和行数级别由“汇总”页阈值驱动，可直接调整阈值。"]];
detail.getRange("A2:P2").format = { fill: paleBlue, font: { color: gray, italic: true }, wrapText: true, verticalAlignment: "center" };
detail.getRange("A2:P2").format.rowHeight = 28;

const detailHeaders = [["容量排名", "表名", "容量级别", "行数级别", "总行数", "行数（亿）", "原始字节数", "磁盘字节数（双副本）", "磁盘容量（TiB）", "容量占比", "累计容量占比", "原始数据量（TiB）", "原始B/行", "磁盘B/行", "压缩比", "原容量文本"]];
detail.getRange("A4:P4").values = detailHeaders;
detail.getRange("A4:P4").format = {
  fill: navy,
  font: { bold: true, color: white },
  horizontalAlignment: "center",
  verticalAlignment: "center",
  wrapText: true,
  borders: { preset: "outside", style: "thin", color: navy },
};
detail.getRange("A4:P4").format.rowHeight = 34;

const startRow = 5;
const endRow = startRow + data.length - 1;
const detailValues = data.map((d) => [
  null, d.table, null, null, d.totalRows, null, d.rawBytes, d.diskBytes,
  null, null, null, null, d.rawBpr, d.diskBpr, d.compressionRatio, d.diskSize,
]);
detail.getRange(`A${startRow}:P${endRow}`).values = detailValues;
detail.getRange(`A${startRow}`).formulas = [["=ROW()-4"]];
detail.getRange(`A${startRow}:A${endRow}`).fillDown();
detail.getRange(`C${startRow}`).formulas = [[`=IF(I${startRow}*1024>='汇总'!$B$14,"超大表",IF(I${startRow}*1024>='汇总'!$B$15,"大表",IF(I${startRow}*1024>='汇总'!$B$16,"中表","小表")))`]];
detail.getRange(`C${startRow}:C${endRow}`).fillDown();
detail.getRange(`D${startRow}`).formulas = [[`=IF(E${startRow}>='汇总'!$H$14,"超高行数",IF(E${startRow}>='汇总'!$H$15,"高行数",IF(E${startRow}>='汇总'!$H$16,"中行数","低行数")))`]];
detail.getRange(`D${startRow}:D${endRow}`).fillDown();
detail.getRange(`F${startRow}`).formulas = [[`=E${startRow}/100000000`]];
detail.getRange(`F${startRow}:F${endRow}`).fillDown();
detail.getRange(`I${startRow}`).formulas = [[`=H${startRow}/1099511627776`]];
detail.getRange(`I${startRow}:I${endRow}`).fillDown();
detail.getRange(`J${startRow}`).formulas = [[`=H${startRow}/SUM($H$${startRow}:$H$${endRow})`]];
detail.getRange(`J${startRow}:J${endRow}`).fillDown();
detail.getRange(`K${startRow}`).formulas = [[`=SUM($J$${startRow}:J${startRow})`]];
detail.getRange(`K${startRow}:K${endRow}`).fillDown();
detail.getRange(`L${startRow}`).formulas = [[`=G${startRow}/1099511627776`]];
detail.getRange(`L${startRow}:L${endRow}`).fillDown();

detail.getRange(`A${startRow}:P${endRow}`).format = {
  font: { size: 10 },
  verticalAlignment: "center",
  borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } },
};
detail.getRange(`A${startRow}:A${endRow}`).format.horizontalAlignment = "center";
detail.getRange(`C${startRow}:D${endRow}`).format.horizontalAlignment = "center";
detail.getRange(`E${startRow}:O${endRow}`).format.horizontalAlignment = "right";
detail.getRange(`E${startRow}:E${endRow}`).format.numberFormat = "#,##0";
detail.getRange(`F${startRow}:F${endRow}`).format.numberFormat = "#,##0.00";
detail.getRange(`G${startRow}:H${endRow}`).format.numberFormat = "#,##0";
detail.getRange(`I${startRow}:I${endRow}`).format.numberFormat = "#,##0.000";
detail.getRange(`J${startRow}:K${endRow}`).format.numberFormat = "0.00%";
detail.getRange(`L${startRow}:L${endRow}`).format.numberFormat = "#,##0.000";
detail.getRange(`M${startRow}:O${endRow}`).format.numberFormat = "#,##0.00";

detail.getRange(`C${startRow}:C${endRow}`).conditionalFormats.add("containsText", { text: "大表", format: { fill: paleOrange, font: { color: "#9C5700", bold: true } } });
detail.getRange(`C${startRow}:C${endRow}`).conditionalFormats.add("containsText", { text: "超大表", format: { fill: "#F8CBAD", font: { color: "#9C0006", bold: true } } });
detail.getRange(`C${startRow}:C${endRow}`).conditionalFormats.add("containsText", { text: "中表", format: { fill: paleBlue, font: { color: navy } } });
detail.getRange(`C${startRow}:C${endRow}`).conditionalFormats.add("containsText", { text: "小表", format: { fill: paleGreen, font: { color: "#375623" } } });
detail.getRange(`J${startRow}:J${endRow}`).conditionalFormats.add("dataBar", { color: blue, gradient: true });

const detailTable = detail.tables.add(`A4:P${endRow}`, true, "BusinessTableDetail");
detailTable.style = "TableStyleMedium2";
detailTable.showBandedRows = true;
detailTable.showFilterButton = true;
detail.freezePanes.freezeRows(4);
detail.freezePanes.freezeColumns(2);

const detailWidths = [10, 35, 11, 12, 16, 12, 21, 22, 16, 12, 15, 17, 13, 13, 11, 15];
for (let c = 0; c < detailWidths.length; c++) detail.getCell(3, c).format.columnWidth = detailWidths[c];

// 汇总页：KPI、阈值表、分布与 Top 10。
summary.showGridLines = false;
titleBand(summary, "A1:M1", "business 数据库表数据量分析摘要");
summary.mergeCells("A2:M2");
summary.getRange("A2").values = [["统计口径：disk_total_bytes 为双副本磁盘总量；主分类按磁盘容量，行数分类作为辅助。来源：国内老集群business库各表数据量统计（双副本）.csv"]];
summary.getRange("A2:M2").format = { fill: paleBlue, font: { color: gray, italic: true }, wrapText: true, verticalAlignment: "center" };
summary.getRange("A2:M2").format.rowHeight = 30;

addCard(summary, "A4:C4", "A5:C6", "表数量", `=COUNTA('明细'!$B$${startRow}:$B$${endRow})`, paleBlue);
addCard(summary, "D4:F4", "D5:F6", "总行数（万亿）", `=SUM('明细'!$E$${startRow}:$E$${endRow})/1000000000000`, paleBlue);
addCard(summary, "G4:I4", "G5:I6", "双副本磁盘总量（TiB）", `=SUM('明细'!$I$${startRow}:$I$${endRow})`, paleOrange);
addCard(summary, "J4:M4", "J5:M6", "Top 5 容量占比", `=SUM('明细'!$J$${startRow}:$J$${startRow + 4})`, paleOrange);
addCard(summary, "A8:C8", "A9:C10", "整体加权压缩比", `=SUM('明细'!$G$${startRow}:$G$${endRow})/SUM('明细'!$H$${startRow}:$H$${endRow})`, paleGreen);
addCard(summary, "D8:F8", "D9:F10", "单副本估算（TiB）", `=SUM('明细'!$I$${startRow}:$I$${endRow})/2`, paleGreen);
addCard(summary, "G8:I8", "G9:I10", "单表磁盘中位数（GiB）", `=MEDIAN('明细'!$I$${startRow}:$I$${endRow})*1024`, paleBlue);
addCard(summary, "J8:M8", "J9:M10", "最大表容量占比", `=MAX('明细'!$J$${startRow}:$J$${endRow})`, paleOrange);
summary.getRange("D5:F6").format.numberFormat = "0.00";
summary.getRange("G5:I6").format.numberFormat = "#,##0.00";
summary.getRange("J5:M6").format.numberFormat = "0.0%";
summary.getRange("A9:C10").format.numberFormat = "0.00";
summary.getRange("D9:F10").format.numberFormat = "#,##0.00";
summary.getRange("G9:I10").format.numberFormat = "#,##0.00";
summary.getRange("J9:M10").format.numberFormat = "0.0%";

sectionHeader(summary, "A12:E12", "容量分层（主口径）");
summary.getRange("A13:E13").values = [["类别", "下限（GiB）", "表数", "磁盘容量（TiB）", "容量占比"]];
summary.getRange("A14:B17").values = [["超大表", 1024], ["大表", 100], ["中表", 10], ["小表", 0]];
for (let row = 14; row <= 17; row++) {
  summary.getRange(`C${row}:E${row}`).formulas = [[
    `=COUNTIF('明细'!$C$${startRow}:$C$${endRow},A${row})`,
    `=SUMIF('明细'!$C$${startRow}:$C$${endRow},A${row},'明细'!$I$${startRow}:$I$${endRow})`,
    `=D${row}/SUM($D$14:$D$17)`,
  ]];
}

sectionHeader(summary, "G12:K12", "行数分层（辅助口径）");
summary.getRange("G13:K13").values = [["类别", "下限（行）", "表数", "行数（万亿）", "行数占比"]];
summary.getRange("G14:H17").values = [["超高行数", 10000000000], ["高行数", 1000000000], ["中行数", 100000000], ["低行数", 0]];
for (let row = 14; row <= 17; row++) {
  summary.getRange(`I${row}:K${row}`).formulas = [[
    `=COUNTIF('明细'!$D$${startRow}:$D$${endRow},G${row})`,
    `=SUMIF('明细'!$D$${startRow}:$D$${endRow},G${row},'明细'!$E$${startRow}:$E$${endRow})/1000000000000`,
    `=J${row}/SUM($J$14:$J$17)`,
  ]];
}

for (const range of ["A13:E13", "G13:K13"]) {
  summary.getRange(range).format = { fill: navy, font: { bold: true, color: white }, horizontalAlignment: "center", verticalAlignment: "center", wrapText: true };
}
for (const range of ["A14:E17", "G14:K17"]) {
  summary.getRange(range).format = { borders: { insideHorizontal: { style: "thin", color: border }, outside: { style: "thin", color: border } }, verticalAlignment: "center" };
}
summary.getRange("B14:B17").format.numberFormat = "#,##0";
summary.getRange("C14:C17").format.numberFormat = "0";
summary.getRange("D14:D17").format.numberFormat = "#,##0.000";
summary.getRange("E14:E17").format.numberFormat = "0.00%";
summary.getRange("H14:H17").format.numberFormat = "#,##0";
summary.getRange("I14:I17").format.numberFormat = "0";
summary.getRange("J14:J17").format.numberFormat = "0.000";
summary.getRange("K14:K17").format.numberFormat = "0.00%";
summary.getRange("A14:A17").format.font = { bold: true };
summary.getRange("G14:G17").format.font = { bold: true };

sectionHeader(summary, "A20:F20", "Top 10 大表（按双副本磁盘容量）");
summary.getRange("A22:F22").values = [["排名", "表名", "磁盘容量（TiB）", "容量占比", "行数（亿）", "压缩比"]];
const top10Formulas = [];
for (let i = 0; i < 10; i++) {
  const sourceRow = startRow + i;
  top10Formulas.push([
    `='明细'!A${sourceRow}`,
    `='明细'!B${sourceRow}`,
    `='明细'!I${sourceRow}`,
    `='明细'!J${sourceRow}`,
    `='明细'!F${sourceRow}`,
    `='明细'!O${sourceRow}`,
  ]);
}
summary.getRange("A23:F32").formulas = top10Formulas;
summary.getRange("A22:F22").format = { fill: navy, font: { bold: true, color: white }, horizontalAlignment: "center", verticalAlignment: "center", wrapText: true };
summary.getRange("A23:F32").format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
summary.getRange("A23:A32").format.horizontalAlignment = "center";
summary.getRange("C23:C32").format.numberFormat = "#,##0.00";
summary.getRange("D23:D32").format.numberFormat = "0.0%";
summary.getRange("E23:E32").format.numberFormat = "#,##0.00";
summary.getRange("F23:F32").format.numberFormat = "0.00";
summary.getRange("C23:C32").conditionalFormats.add("dataBar", { color: blue, gradient: true });

const chart = summary.charts.add("bar", summary.getRange("B22:C32"));
chart.title = "Top 10 表磁盘容量（TiB，双副本）";
chart.hasLegend = false;
chart.setPosition("H20", "M36");
if (chart.series.items.length > 0) chart.series.items[0].fill = blue;

sectionHeader(summary, "A35:M35", "核心结论与建议");
const insights = [
  `1. 容量高度集中：最大表占 ${topShare(1).toLocaleString("zh-CN", { style: "percent", maximumFractionDigits: 1 })}，Top 3 占 ${topShare(3).toLocaleString("zh-CN", { style: "percent", maximumFractionDigits: 1 })}，Top 5 占 ${topShare(5).toLocaleString("zh-CN", { style: "percent", maximumFractionDigits: 1 })}，Top 10 占 ${topShare(10).toLocaleString("zh-CN", { style: "percent", maximumFractionDigits: 1 })}。`,
  `2. 优先治理 network_security_log_local 与 http_log_local：两表合计 ${(data[0].diskBytes + data[1].diskBytes) / TIB < 1000 ? ((data[0].diskBytes + data[1].diskBytes) / TIB).toFixed(2) : Math.round((data[0].diskBytes + data[1].diskBytes) / TIB)} TiB，占 ${topShare(2).toLocaleString("zh-CN", { style: "percent", maximumFractionDigits: 1 })}。`,
  `3. 行数与容量并不等价：dns_log_local 行数最多（${(data.find((d) => d.table === "dns_log_local").totalRows / 1e8).toFixed(2)} 亿行），但容量为 ${(data.find((d) => d.table === "dns_log_local").diskBytes / TIB).toFixed(2)} TiB；network_security_log_local 容量最大（${(data[0].diskBytes / TIB).toFixed(2)} TiB）。`,
  `4. ssl_tls_log_local 的磁盘行宽为 ${data.find((d) => d.table === "ssl_tls_log_local").diskBpr.toFixed(2)} B/行、压缩比 ${data.find((d) => d.table === "ssl_tls_log_local").compressionRatio.toFixed(2)}，在头部表中较宽且压缩偏低，适合检查字段裁剪、编码、索引和冷热分层。`,
  `5. ${smallTables.length} 张小表合计仅 ${(smallDisk / TIB).toFixed(3)} TiB（${(smallDisk / GIB).toFixed(2)} GiB），只占 ${(smallDisk / totalDisk).toLocaleString("zh-CN", { style: "percent", minimumFractionDigits: 3, maximumFractionDigits: 3 })}；若目标是降容量，小表整体优先级较低。`,
];
for (let i = 0; i < insights.length; i++) {
  const row = 36 + i;
  summary.mergeCells(`A${row}:M${row}`);
  summary.getRange(`A${row}`).values = [[insights[i]]];
  summary.getRange(`A${row}:M${row}`).format = { fill: i % 2 === 0 ? paleGray : white, font: { color: "#344054" }, wrapText: true, verticalAlignment: "center" };
  summary.getRange(`A${row}:M${row}`).format.rowHeight = 30;
}

summary.freezePanes.freezeRows(2);
const summaryWidths = [11, 17, 16, 14, 14, 12, 12, 17, 13, 14, 13, 13, 13];
for (let c = 0; c < summaryWidths.length; c++) summary.getCell(12, c).format.columnWidth = summaryWidths[c];
summary.getRange("B23:B32").format.columnWidth = 36;

// 大小表清单：引用明细页，便于直接查看两端分布。
lists.showGridLines = false;
titleBand(lists, "A1:G1", `大表清单（≥100 GiB，共 ${largeTables.length} 张）`);
titleBand(lists, "I1:O1", `小表清单（<10 GiB，共 ${smallTables.length} 张）`);
lists.mergeCells("A2:G2");
lists.getRange("A2").values = [["包含“超大表”和“大表”，按磁盘容量降序；容量为双副本口径。"]];
lists.mergeCells("I2:O2");
lists.getRange("I2").values = [[`小表合计 ${(smallDisk / TIB).toFixed(3)} TiB，仅占 ${(smallDisk / totalDisk * 100).toFixed(3)}%。`]];
lists.getRange("A2:G2").format = { fill: paleOrange, font: { color: gray, italic: true }, wrapText: true };
lists.getRange("I2:O2").format = { fill: paleGreen, font: { color: gray, italic: true }, wrapText: true };

const listHeaders = [["容量级别", "容量排名", "表名", "总行数", "磁盘容量（TiB）", "容量占比", "压缩比"]];
lists.getRange("A4:G4").values = listHeaders;
lists.getRange("I4:O4").values = listHeaders;
for (const range of ["A4:G4", "I4:O4"]) lists.getRange(range).format = { fill: navy, font: { bold: true, color: white }, horizontalAlignment: "center", verticalAlignment: "center", wrapText: true };

const largeFormulas = [];
for (let i = 0; i < largeTables.length; i++) {
  const r = startRow + i;
  largeFormulas.push([`='明细'!C${r}`, `='明细'!A${r}`, `='明细'!B${r}`, `='明细'!E${r}`, `='明细'!I${r}`, `='明细'!J${r}`, `='明细'!O${r}`]);
}
lists.getRange(`A5:G${4 + largeTables.length}`).formulas = largeFormulas;

const smallStartIndex = data.findIndex((d) => d.diskBytes / GIB < 10);
const smallFormulas = [];
for (let i = 0; i < smallTables.length; i++) {
  const r = startRow + smallStartIndex + i;
  smallFormulas.push([`='明细'!C${r}`, `='明细'!A${r}`, `='明细'!B${r}`, `='明细'!E${r}`, `='明细'!I${r}`, `='明细'!J${r}`, `='明细'!O${r}`]);
}
lists.getRange(`I5:O${4 + smallTables.length}`).formulas = smallFormulas;

for (const range of [`A5:G${4 + largeTables.length}`, `I5:O${4 + smallTables.length}`]) {
  lists.getRange(range).format = { borders: { insideHorizontal: { style: "thin", color: "#E5E7EB" } }, verticalAlignment: "center" };
}
lists.getRange(`B5:B${4 + largeTables.length}`).format.horizontalAlignment = "center";
lists.getRange(`J5:J${4 + smallTables.length}`).format.horizontalAlignment = "center";
lists.getRange(`D5:D${4 + largeTables.length}`).format.numberFormat = "#,##0";
lists.getRange(`L5:L${4 + smallTables.length}`).format.numberFormat = "#,##0";
lists.getRange(`E5:E${4 + largeTables.length}`).format.numberFormat = "#,##0.000";
lists.getRange(`M5:M${4 + smallTables.length}`).format.numberFormat = "#,##0.000";
lists.getRange(`F5:F${4 + largeTables.length}`).format.numberFormat = "0.00%";
lists.getRange(`N5:N${4 + smallTables.length}`).format.numberFormat = "0.00%";
lists.getRange(`G5:G${4 + largeTables.length}`).format.numberFormat = "0.00";
lists.getRange(`O5:O${4 + smallTables.length}`).format.numberFormat = "0.00";
lists.getRange(`A5:A${4 + largeTables.length}`).conditionalFormats.add("containsText", { text: "大表", format: { fill: paleOrange, font: { color: "#9C5700", bold: true } } });
lists.getRange(`A5:A${4 + largeTables.length}`).conditionalFormats.add("containsText", { text: "超大表", format: { fill: "#F8CBAD", font: { color: "#9C0006", bold: true } } });
lists.getRange(`I5:I${4 + smallTables.length}`).conditionalFormats.add("containsText", { text: "小表", format: { fill: paleGreen, font: { color: "#375623" } } });

const largeListTable = lists.tables.add(`A4:G${4 + largeTables.length}`, true, "LargeTables");
largeListTable.style = "TableStyleMedium9";
const smallListTable = lists.tables.add(`I4:O${4 + smallTables.length}`, true, "SmallTables");
smallListTable.style = "TableStyleMedium4";
lists.freezePanes.freezeRows(4);
lists.getRange("A:O");
const listWidths = { A: 11, B: 10, C: 35, D: 16, E: 17, F: 13, G: 11, H: 3, I: 11, J: 10, K: 35, L: 16, M: 17, N: 13, O: 11 };
for (const [col, width] of Object.entries(listWidths)) lists.getRange(`${col}4`).format.columnWidth = width;

// Compact verification before export.
const checks = [];
checks.push((await workbook.inspect({ kind: "table", range: "汇总!A1:M40", include: "values,formulas", tableMaxRows: 40, tableMaxCols: 13, maxChars: 12000 })).ndjson);
checks.push((await workbook.inspect({ kind: "table", range: `明细!A1:P${endRow}`, include: "values,formulas", tableMaxRows: 12, tableMaxCols: 16, maxChars: 9000 })).ndjson);
checks.push((await workbook.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A", options: { useRegex: true, maxResults: 300 }, summary: "final formula error scan" })).ndjson);
console.log(checks.join("\n"));

await fs.mkdir(previewDir, { recursive: true });
for (const [sheetName, range, filename] of [
  ["汇总", "A1:M40", "summary.png"],
  ["明细", `A1:P${endRow}`, "detail.png"],
  ["大小表清单", `A1:O${Math.max(4 + smallTables.length, 4 + largeTables.length)}`, "lists.png"],
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
  tableCount: data.length,
  totalRows,
  totalDiskTiB: totalDisk / TIB,
  totalRawTiB: totalRaw / TIB,
  weightedCompression: totalRaw / totalDisk,
  top1Share: topShare(1),
  top3Share: topShare(3),
  top5Share: topShare(5),
  top10Share: topShare(10),
  largeCount: largeTables.length,
  smallCount: smallTables.length,
  smallDiskTiB: smallDisk / TIB,
}, null, 2));
