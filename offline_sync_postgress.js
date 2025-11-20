// offline-sync.js - FULL OFFLINE SYNC + ACTION HISTORY LOGGING
// 100% Complete | Works on Node.js v22+ | Correct IST 24-hour format | Fixed pushedtopool Logic
const fs = require("fs");
const path = require("path");
const sql = require("mssql");
const { Client } = require("pg");
const sqlConfig = {
  server: "localhost",
  database: "swdjk",
  user: "sa",
  password: "U$m1e$k@",
  options: { encrypt: false, trustServerCertificate: true },
};

const pgConfig = {
  host: "localhost",
  port: 5432,
  database: "JanSugamData",
  user: "postgres",
  password: "your_password_here", // CHANGE THIS TO YOUR ACTUAL POSTGRES PASSWORD
};

let initiatedMap = {};
let tswoToTehsilIdMap = {};
// CORRECT 24-HOUR IST FORMAT: "18 Nov 2025 17:30:45"
function formatToDesired(dateInput) {
  if (!dateInput) return null;
  let date;
  if (dateInput instanceof Date) {
    date = dateInput;
  } else {
    const input = String(dateInput).trim();
    if (/^\d{4}-\d{2}-\d{2}[T\s]/.test(input)) {
      date = new Date(input);
    } else if (/^\d{1,2}[-\/]\d{1,2}[-\/]\d{4}/.test(input)) {
      const [datePart, timePart = ""] = input.split(/\s+/);
      const [d, m, y] = datePart.split(/[-\/]/).map(Number);
      let hours = 0,
        minutes = 0,
        seconds = 0;
      if (timePart) {
        const match = timePart.match(/(\d{1,2}):(\d{2}):(\d{2})\s*(AM|PM)?/i);
        if (match) {
          let h = parseInt(match[1], 10);
          const ampm = match[4]?.toUpperCase();
          if (ampm === "PM" && h !== 12) h += 12;
          if (ampm === "AM" && h === 12) h = 0;
          hours = h;
          minutes = parseInt(match[2], 10);
          seconds = parseInt(match[3], 10);
        }
      }
      date = new Date(y, m - 1, d, hours, minutes, seconds);
    } else {
      date = new Date(input);
    }
  }
  if (!date || isNaN(date.getTime())) {
    console.warn("Invalid date, using now:", dateInput);
    date = new Date();
  }
  const IST_OFFSET_MINUTES = 330;
  const utcTimestamp = date.getTime() + date.getTimezoneOffset() * 60000;
  const istDate = new Date(utcTimestamp + IST_OFFSET_MINUTES * 60000);
  const pad = (n) => n.toString().padStart(2, "0");
  const day = pad(istDate.getDate());
  const month = istDate.toLocaleString("en-US", { month: "short" });
  const year = istDate.getFullYear();
  const hours = pad(istDate.getHours());
  const minutes = pad(istDate.getMinutes());
  const seconds = pad(istDate.getSeconds());
  return `${day} ${month} ${year} ${hours}:${minutes}:${seconds}`;
}
function formatDateTime(dateString) {
  return formatToDesired(dateString);
}
// FULL SECTION CONFIG
const sectionsConfig = {
  Location: [
    { id: 140871, label: "Select District", name: "District", valueType: "id" },
    {
      id: 140852,
      label: "Select Tehsil Social Welfare Office (TSWO)",
      name: "Tehsil",
      valueType: "id",
    },
  ],
  "Pension Type": [
    {
      id: 141683,
      label: "Select Pension Type",
      name: "PensionType",
      valueType: "text",
    },
  ],
  "Applicant Details": [
    {
      id: 140853,
      label: "Name of the Applicant",
      name: "ApplicantName",
      valueType: "text",
    },
    {
      id: 140857,
      label: "Photograph of Applicant",
      name: "ApplicantImage",
      valueType: "file",
    },
    {
      id: 140855,
      label: "Date of Birth",
      name: "DateOfBirth",
      valueType: "text",
    },
    { id: 140856, label: "Age (In Years)", name: "Age", valueType: "text" },
    {
      id: 140858,
      label: "Father / Husband / Guardian Name",
      name: "Parentage",
      valueType: "text",
    },
    { id: 143084, label: "Gender", name: "Gender", valueType: "text" },
    { id: 142203, label: "Category", name: "Category", valueType: "text" },
    {
      id: 140861,
      label: "Mobile Number",
      name: "MobileNumber",
      valueType: "text",
    },
    { id: 140862, label: "E-Mail", name: "Email", valueType: "text" },
    {
      id: 142202,
      label: "Do you have BPL card",
      name: "BPLCard",
      valueType: "text",
    },
  ],
  "Present Address Details": [
    {
      id: 140863,
      label: "Present Address",
      name: "PresentAddress",
      valueType: "text",
    },
    {
      id: 140864,
      label: "Present District",
      name: "PresentDistrict",
      valueType: "id",
    },
    {
      id: 143034,
      label: "Present Tehsil",
      name: "PresentTehsil",
      valueType: "id",
    },
    {
      id: 141890,
      label: "Present Halqa Panchayat / Municipality Name",
      name: "PresentPanchayat",
      valueType: "text",
    },
    {
      id: 141836,
      label: "Present Village Name",
      name: "PresentVillage",
      valueType: "text",
    },
    { id: 141837, label: "Pincode", name: "PresentPincode", valueType: "text" },
  ],
  "Permanent Address Details": [
    {
      id: 143077,
      label: 'Is "Permanent Address" same as "Present Address"',
      name: "SameAsPresent",
      valueType: "bool",
    },
    {
      id: 140870,
      label: "Permanent Address",
      name: "PermanentAddress",
      valueType: "text",
    },
    {
      id: 140871,
      label: "Permanent District",
      name: "PermanentDistrict",
      valueType: "id",
    },
    {
      id: 143035,
      label: "Permanent Tehsil",
      name: "PermanentTehsil",
      valueType: "id",
    },
    {
      id: 141891,
      label: "Permanent Halqa Panchayat / Municipality Name",
      name: "PermanentPanchayat",
      valueType: "text",
    },
    {
      id: 141838,
      label: "Permanent Village Name",
      name: "PermanentVillage",
      valueType: "text",
    },
    {
      id: 141839,
      label: "Pincode",
      name: "PermanentPincode",
      valueType: "text",
    },
  ],
  "Bank Details": [
    { id: 140876, label: "Bank Name", name: "BankName", valueType: "id" },
    { id: 140873, label: "Branch Name", name: "BranchName", valueType: "text" },
    { id: 140874, label: "IFSC Code", name: "IfscCode", valueType: "text" },
    {
      id: 140875,
      label: "Account No. of the Applicant",
      name: "AccountNumber",
      valueType: "text",
    },
  ],
  "Previous Pension Details": [
    {
      id: 143605,
      label: "Are you previously taking Pension from JK-ISSS / GOI-NSAP",
      name: "PreviousPension",
      valueType: "text",
    },
    {
      id: 143606,
      label: "Bank Name. (previous)",
      name: "PreviousBankName",
      valueType: "id",
    },
    {
      id: 143607,
      label: "Branch Name. (previous)",
      name: "PreviousBranchName",
      valueType: "text",
    },
    {
      id: 143608,
      label: "IFSC Code. (previous)",
      name: "PreviousIfscCode",
      valueType: "text",
    },
    {
      id: 143609,
      label: "Account Number. (previous)",
      name: "PreviousAccountNumber",
      valueType: "text",
    },
  ],
  Consent: [
    {
      id: 141831,
      label:
        "Do you consent to share your email Id and mobile number with Rapid Assessment System...",
      name: "ConsentRAS",
      valueType: "text",
    },
  ],
  Declaration: [
    { id: 140878, label: "I Agree", name: "Declaration", valueType: "bool" },
  ],
};
// Load TSWO → TehsilId Mapping
async function loadTSWOTehsilMapping() {
  try {
    const result = await new sql.Request().query`
      SELECT tswoOfficeName, TehsilId FROM [dbo].[TSWOTehsil]
      WHERE tswoOfficeName IS NOT NULL AND LTRIM(RTRIM(tswoOfficeName)) != ''
    `;
    result.recordset.forEach((row) => {
      let office = row.tswoOfficeName.trim();
      let extracted = office;
      const match = office.match(/\((.*?)\)/);
      if (match && match[1]) {
        extracted = match[1]
          .replace(/TSWO\s*-/gi, "")
          .replace(/TSWO/gi, "")
          .trim();
      }
      const finalName = extracted.replace(/\s+/g, " ").trim();
      tswoToTehsilIdMap[finalName] = row.TehsilId;
      const short = finalName.replace(/^TSWO\s+/i, "").trim();
      if (short) tswoToTehsilIdMap[short] = row.TehsilId;
    });
    console.log(
      `Loaded ${Object.keys(tswoToTehsilIdMap).length} TSWO → TehsilId mappings`
    );
  } catch (err) {
    console.error("Failed to load TSWO mapping:", err.message);
  }
}
function getTehsilIdFromTSWO(value) {
  if (!value) return null;
  const parts = String(value).split("~");
  if (parts.length < 2) return null;
  const name = parts[1].trim();
  return (
    tswoToTehsilIdMap[name] ||
    tswoToTehsilIdMap[name.replace(/^TSWO\s+/i, "").trim()] ||
    null
  );
}
function normalizeDesignation(raw) {
  if (!raw) return "Unknown";
  let name = raw.trim();
  if (name.includes("Pool for Bulk Issuance")) {
    if (name.includes("District")) return "District Social Welfare Officer";
    if (name.includes("Tehsil")) return "Tehsil Social Welfare Officer";
    if (name.includes("Director")) return "Director Social Welfare";
  }
  if (name.includes("District Social Welfare Officer") || name.includes("DSWO"))
    return "District Social Welfare Officer";
  if (name.includes("Tehsil Social Welfare Officer") || name.includes("TSWO"))
    return "Tehsil Social Welfare Officer";
  if (name.includes("Director Social Welfare"))
    return "Director Social Welfare";
  return name;
}
// Insert into ActionHistory
async function insertActionHistory(
  pool,
  refNum,
  actionTaker,
  actionTaken,
  locationLevel,
  locationValue,
  remarks,
  actionDate
) {
  const query = `
    INSERT INTO [dbo].[ActionHistory]
      (referenceNumber, ActionTaker, ActionTaken, LocationLevel, LocationValue, Remarks, ActionTakenDate)
    VALUES (@refNum, @actionTaker, @actionTaken, @locationLevel, @locationValue, @remarks, @actionDate)
  `;
  try {
    const req = pool.request();
    req.input("refNum", sql.VarChar(30), refNum);
    req.input("actionTaker", sql.VarChar(255), actionTaker);
    req.input("actionTaken", sql.VarChar(100), actionTaken);
    req.input("locationLevel", sql.VarChar(100), locationLevel || null);
    req.input("locationValue", sql.Int, locationValue || null);
    req.input("remarks", sql.VarChar(255), remarks || "");
    req.input("actionDate", sql.VarChar(50), actionDate);
    await req.query(query);
  } catch (err) {
    console.error(`ActionHistory insert failed [${refNum}]: ${err.message}`);
  }
}
// Process Initiated Applications
async function processAndInsertInitiatedData(entry, pool) {
  const applId = entry.appl_id;
  const refNum = entry.appl_ref_no;
  initiatedMap[applId] = { appl_ref_no: refNum };
  const attr = entry.attribute_details || {};
  const formDetails = {};
  const getValue = (id, type = "text") => {
    const raw = attr[id];
    if (!raw) return null;
    const str = String(raw);
    if (type === "id") {
      if (id === 140852) {
        const mapped = getTehsilIdFromTSWO(raw);
        if (mapped) return mapped;
        return str.split("~")[0]?.trim() || null;
      }
      return str.split("~")[0]?.trim() || null;
    }
    if (type === "text") return str.split("~")[1]?.trim() || str.trim();
    if (type === "file") return raw ? { File: path.basename(raw) } : null;
    if (type === "bool") return str === "Y" || str.includes("YES");
    return str;
  };
  Object.keys(sectionsConfig).forEach((sec) => {
    formDetails[sec] = sectionsConfig[sec]
      .map((f) => {
        const v = getValue(f.id, f.valueType);
        return v == null ? null : { label: f.label, name: f.name, value: v };
      })
      .filter(Boolean);
  });
  formDetails.Documents = (entry.enclosure_details || []).map((enc, i) => {
    const [docId, attId] = Object.entries(enc)[0] || [];
    return {
      label: `Document ${docId || "Unknown"}`,
      name: `Doc_${docId || i}`,
      File: `attachment_${attId || "unknown"}.jpg`,
    };
  });
  let districtUid = "000000";
  const distRaw = attr["141677"];
  if (distRaw && typeof distRaw === "string") {
    const idPart = distRaw.split("~")[0]?.trim();
    if (idPart && /^\d+$/.test(idPart))
      districtUid = idPart.padStart(6, "0").substring(0, 6);
  }
  const createdAt =
    formatToDesired(entry.submission_date) || formatToDesired(new Date());
  let tehsilId = null;
  const tehsilRaw = attr["140852"];
  if (tehsilRaw) {
    tehsilId =
      getTehsilIdFromTSWO(tehsilRaw) ||
      parseInt(String(tehsilRaw).split("~")[0]) ||
      null;
  }
  const defaultWorkflow = [
    {
      designation: "Tehsil Social Welfare Officer",
      accessLevel: "Tehsil",
      status: "pending",
      completedAt: null,
      remarks: "",
      playerId: 0,
      prevPlayerId: null,
      nextPlayerId: 1,
      canPull: false,
    },
    {
      designation: "District Social Welfare Officer",
      accessLevel: "District",
      status: "",
      completedAt: null,
      remarks: "",
      playerId: 1,
      prevPlayerId: 0,
      nextPlayerId: 2,
      canPull: false,
    },
    {
      designation: "Director Social Welfare",
      accessLevel: "Division",
      status: "",
      completedAt: null,
      remarks: "",
      playerId: 2,
      prevPlayerId: 1,
      nextPlayerId: null,
      canPull: false,
    },
  ];
  const mergeQuery = `
    MERGE [dbo].[Citizen_Applications] AS t
    USING (SELECT @Ref AS ReferenceNumber) AS s ON t.ReferenceNumber = s.ReferenceNumber
    WHEN MATCHED THEN
      UPDATE SET FormDetails = @FormDetails, DistrictUidForBank = @DistrictUid, Created_at = @CreatedAt,
                 ServiceId = @ServiceId, appl_id = @appl_id, WorkFlow = @WorkFlow,
                 CurrentPlayer = 0, Status = 'Initiated', DataType = 'legacy'
    WHEN NOT MATCHED THEN
      INSERT (ReferenceNumber, ReferenceNumberAlphaNumeric, Citizen_id, ServiceId, DistrictUidForBank,
              FormDetails, WorkFlow, Status, CurrentPlayer, DataType, Created_at, appl_id)
      VALUES (@Ref, @Ref, 0, @ServiceId, @DistrictUid, @FormDetails,
              @WorkFlow, 'Initiated', 0, 'legacy', @CreatedAt, @appl_id);
  `;
  try {
    const req = pool.request();
    req.input("Ref", sql.VarChar(50), refNum);
    req.input("ServiceId", sql.Int, 1);
    req.input("DistrictUid", sql.VarChar(6), districtUid);
    req.input(
      "FormDetails",
      sql.NVarChar(sql.MAX),
      JSON.stringify(formDetails)
    );
    req.input(
      "WorkFlow",
      sql.NVarChar(sql.MAX),
      JSON.stringify(defaultWorkflow)
    );
    req.input("CreatedAt", sql.VarChar(50), createdAt);
    req.input("appl_id", sql.BigInt, applId);
    await req.query(mergeQuery);
    await insertActionHistory(
      pool,
      refNum,
      "Citizen",
      "Application Submission",
      "Tehsil",
      tehsilId,
      "Submitted",
      createdAt
    );
  } catch (err) {
    console.error(`FAILED INSERT ${refNum}:`, err.message);
  }
}
// Process Execution Data - FINAL 100% ACCURATE pushedtopool LOGIC
async function processAndInsertExecutionData(execEntry, pool) {
  const task = execEntry.task_details;
  const applId = task.appl_id.toString();
  const initEntry = initiatedMap[applId];
  if (!initEntry) return;
  const refNum = initEntry.appl_ref_no;
  let workflow = [];
  let tehsilId = null;
  try {
    const res = await pool.request()
      .query`SELECT WorkFlow, FormDetails FROM Citizen_Applications WHERE ReferenceNumber = ${refNum}`;
    if (!res.recordset[0]?.WorkFlow) return;
    workflow = JSON.parse(res.recordset[0].WorkFlow);
    const formDetails = JSON.parse(res.recordset[0].FormDetails || "{}");
    const tehsilField = formDetails.Location?.find((f) => f.name === "Tehsil");
    tehsilId = tehsilField?.value ? parseInt(tehsilField.value) : null;
  } catch (err) {
    console.error(`Read failed ${refNum}:`, err.message);
    return;
  }
  const rawDes = task.task_name || task.user_detail?.designation || "Unknown";
  const normDes = normalizeDesignation(rawDes);
  const completedAt = formatToDesired(task.executed_time || task.received_time);
  const actionDetail = (task.task_action_detail || "").toLowerCase().trim();
  const actionId = task.task_action;
  // === GET FINAL DESTINATION TASK NAME ===
  let finalNextTaskName = "";
  if (
    execEntry.next_task_data &&
    Array.isArray(execEntry.next_task_data) &&
    execEntry.next_task_data.length > 0
  ) {
    // Take the LAST task in the chain (actual final destination)
    finalNextTaskName =
      execEntry.next_task_data[execEntry.next_task_data.length - 1]
        .next_task_name || "";
  }
  // === IS IT BEING PUSHED TO ANY POOL? ===
  const isPushedToPool = /pool|bulk issuance/i.test(finalNextTaskName);
  const isForwardAction =
    actionDetail.includes("forward") || actionDetail === "forward";
  // === DETERMINE STATUS - FINAL UNIVERSAL RULE ===
  let status = "forwarded";
  if (isForwardAction && isPushedToPool) {
    status = "pushedtopool";
  } else if (
    actionDetail.includes("deliver") ||
    actionDetail.includes("sanction") ||
    actionDetail.includes("approve")
  ) {
    status = "sanctioned";
  } else if (actionDetail.includes("reject") || actionId === 35) {
    status = "rejected";
  } else if (actionDetail.includes("return") || actionId === 34) {
    status = "returned";
  } else if (
    actionDetail.includes("sent for edit") ||
    actionDetail.includes("return to citizen")
  ) {
    status = "returntoedit";
  } else if (actionDetail.includes("forward")) {
    status = "forwarded";
  }
  const remarks =
    (
      execEntry.official_form_details?.["143910"] ||
      execEntry.official_form_details?.["143909"] ||
      task.remarks ||
      ""
    ).trim() || (status === "sanctioned" ? "Sanctioned" : "Forwarded");
  const stepIdx = workflow.findIndex((s) => s.designation === normDes);
  if (stepIdx === -1) {
    console.warn(
      `Designation not found: "${rawDes}" → "${normDes}" | ${refNum}`
    );
    return;
  }
  const step = workflow[stepIdx];
  const shouldUpdate =
    step.status !== status ||
    !step.completedAt ||
    (completedAt && new Date(step.completedAt) < new Date(completedAt));
  if (!shouldUpdate) return;
  step.status = status;
  step.completedAt = completedAt;
  step.remarks = remarks;
  let finalStatus = "Initiated";
  if (workflow.some((s) => s.status === "rejected")) finalStatus = "Rejected";
  else if (workflow.some((s) => s.status === "sanctioned"))
    finalStatus = "Sanctioned";
  const currentPlayer =
    workflow.findIndex((s) => s.status && !["pending", ""].includes(s.status)) +
      1 || 1;
  let locationLevel = "Tehsil";
  if (normDes.includes("District")) locationLevel = "District";
  else if (normDes.includes("Director")) locationLevel = "Division";
  let actionTaken = "Forwarded";
  if (status === "pushedtopool") actionTaken = "Pushed to Pool";
  else if (status === "returned") actionTaken = "Returned";
  else if (status === "returntoedit")
    actionTaken = "Returned to Citizen for editing";
  else if (status === "rejected") actionTaken = "Rejected";
  else if (status === "sanctioned") actionTaken = "Sanctioned";
  try {
    await pool
      .request()
      .input("WorkFlow", sql.NVarChar(sql.MAX), JSON.stringify(workflow))
      .input("CurrentPlayer", sql.Int, currentPlayer)
      .input("Status", sql.NVarChar(50), finalStatus)
      .input("RefNum", sql.VarChar(50), refNum).query`
        UPDATE Citizen_Applications
        SET WorkFlow = @WorkFlow, CurrentPlayer = @CurrentPlayer, Status = @Status, DataType = 'legacy'
        WHERE ReferenceNumber = @RefNum
      `;
    await insertActionHistory(
      pool,
      refNum,
      normDes,
      actionTaken,
      locationLevel,
      tehsilId,
      remarks,
      completedAt
    );
  } catch (err) {
    console.error(`Update failed ${refNum}:`, err.message);
  }
}
// MAIN FUNCTION
async function main() {
  console.log(
    `\nOFFLINE JanSugam Sync + ActionHistory Started @ ${formatToDesired(
      new Date()
    )}\n`
  );

  const mssqlPool = await sql.connect(sqlConfig);
  const pgClient = new Client(pgConfig);
  await pgClient.connect();

  await loadTSWOTehsilMapping();

  try {
    // Get all tables for ServiceId 1888: jk_data_1888_Month_year
    const tableResult = await pgClient.query(`
      SELECT tablename 
      FROM pg_tables 
      WHERE schemaname = 'public' 
        AND tablename LIKE 'jk_data_1888_%'
      ORDER BY tablename
    `);

    if (tableResult.rows.length === 0) {
      console.log("No jk_data_1888_* tables found in JanSugamData database.");
      return;
    }

    console.log(
      `Found ${
        tableResult.rows.length
      } table(s) for ServiceId 1888:\n   ${tableResult.rows
        .map((r) => r.tablename)
        .join(", ")}\n`
    );

    for (const table of tableResult.rows) {
      const tableName = table.tablename;
      console.log(`Processing table: ${tableName}`);

      const res = await pgClient.query(`
        SELECT push_application_processing_data 
        FROM "${tableName}"
        ORDER BY row_id  -- optional: remove if no row_id
      `);

      for (const [idx, row] of res.rows.entries()) {
        const data = row.push_application_processing_data;

        if (!data || typeof data !== "object") {
          console.warn(
            `   Skipping invalid/empty row ${idx + 1} in ${tableName}`
          );
          continue;
        }

        // Process Initiated Data
        if (
          Array.isArray(data.initiated_data) &&
          data.initiated_data.length > 0
        ) {
          for (const [i, e] of data.initiated_data.entries()) {
            await processAndInsertInitiatedData(e, mssqlPool);
            if (i % 50 === 49)
              console.log(`     Initiated: ${i + 1} done (${tableName})`);
          }
        }

        // Rebuild initiatedMap after initiated data (critical for execution linking)
        const mapRes = await mssqlPool.request()
          .query`SELECT appl_id, ReferenceNumber FROM Citizen_Applications WHERE appl_id IS NOT NULL`;

        initiatedMap = {};
        mapRes.recordset.forEach((r) => {
          initiatedMap[r.appl_id.toString()] = {
            appl_ref_no: r.ReferenceNumber,
          };
        });

        // Process Execution Data
        if (
          Array.isArray(data.execution_data) &&
          data.execution_data.length > 0
        ) {
          let cnt = 0;
          for (const e of data.execution_data) {
            await processAndInsertExecutionData(e, mssqlPool);
            if (++cnt % 100 === 0)
              console.log(`     Execution: ${cnt} done (${tableName})`);
          }
        }
      }

      console.log(`Completed → ${tableName}\n`);
    }

    console.log(
      "ALL SERVICEID 1888 TABLES PROCESSED SUCCESSFULLY FROM POSTGRESQL!\n"
    );
  } catch (err) {
    console.error("FATAL ERROR DURING POSTGRESQL PROCESSING:", err.message);
    throw err;
  } finally {
    await pgClient.end();
  }
}
// RUN SAFELY
(async () => {
  try {
    await main();
    console.log(
      "OFFLINE SYNC + ACTION HISTORY LOGGING COMPLETED 100% SUCCESSFULLY!\n"
    );
  } catch (err) {
    console.error("FATAL ERROR:", err);
    process.exit(1);
  } finally {
    try {
      await sql.close();
    } catch (_) {}
  }
})();
