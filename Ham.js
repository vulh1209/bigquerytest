
const Excel = require("exceljs");


function changeRowsToDict(worksheet) {
    let dataArray = [];
    let keys = [];
    worksheet.eachRow(function (row, rowNumber) {
        if (rowNumber == 1) {
            keys = row.values;
        }
        else {
            let rowDict = cellValueToDict2(keys, row);
            dataArray.push(rowDict);
        }
    });
    return dataArray;
}

function cellValueToDict(keys, rowValue) {
    let rowDict = {};
    keys.forEach((value, index) => {
        rowDict[value] = rowValue[index];
    });
    return rowDict;
}

/* keys: {id,name,phone}, rowValue：每一行的值数组， 执行次数3次 */
function cellValueToDict2(keys, row) {
    let data = {};
    row.eachCell(function (cell, colNumber) {
        var value = cell.value;
        if (typeof value == "object") value = value.text;
        data[keys[colNumber]] = value;
    });
    return data;
}

const logs = () => { console.log('test'); }

const LocDanhsach = async (filepath) => {
    const workbook = new Excel.Workbook();
    await workbook.xlsx.readFile(filepath);
    const worksheet_crm = workbook.getWorksheet("CRM_Leads");
    const worksheet_edm = workbook.getWorksheet("EDM_Leads");
    worksheet_crm.columns = [
        { header: "FullName", key: "fullname" },
        { header: "scoreFullname", key: "scoreFullname" },
        { header: "email", key: "email" },
        { header: "scoreEmail", key: "scoreEmail" },
        { header: "phone", key: "phone" },
        { header: "scorePhone", key: "scorePhone" },
        { header: "src", key: "src" },
        { header: "scoreSRC", key: "scoreSRC" },
        { header: "totalScore", key: "totalScore" },
        { header: "note", key: "note" },
        { header: "dateStamp", key: "dateStamp" },
    ];
    worksheet_edm.columns = [
        { header: "FullName", key: "fullname" },
        { header: "scoreFullname", key: "scoreFullname" },
        { header: "email", key: "email" },
        { header: "scoreEmail", key: "scoreEmail" },
        { header: "phone", key: "phone" },
        { header: "scorePhone", key: "scorePhone" },
        { header: "src", key: "src" },
        { header: "scoreSRC", key: "scoreSRC" },
        { header: "totalScore", key: "totalScore" },
        { header: "note", key: "note" },
        { header: "dateStamp", key: "dateStamp" },
    ];
    // const rows = worksheet_crm.getRows(1, worksheet_crm.actualRowCount);
    // const json = JSON.stringify(worksheet_crm.model.rows);

    let dataArray = changeRowsToDict(worksheet_crm);
    console.log(JSON.stringify(dataArray));
    return dataArray

    // const rows = changeRowsToDict(worksheet_crm);
    // worksheet_crm.eachCell(function (cell, colNumber) {
    //     console.log('Cell ' + colNumber + ' = ' + JSON.stringify(cell.value));
    // })
}

//export modules
module.exports = {
    logs,
    LocDanhsach
}

// module.exports = function ( firstArg, secondArg ) {

//     function firstFunction ( ) { ... }

//     function secondFunction ( ) { ... }

//     function thirdFunction ( ) { ... }

//       return { firstFunction: firstFunction, secondFunction: secondFunction,
//  thirdFunction: thirdFunction };

//     }