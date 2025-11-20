// server.js - JSON ONLY DOWNLOADER (No DB Operations)
// JanSugam → 4 Dates per configId → Cycle Through configIds
// Date: 14 Nov 2025
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// ==================== CONFIGURATION ====================
const url = "https://jansugam.jk.gov.in/api/v1/service-applications/data";

const configIds = [161, 198, 205]; // Must be in order

// === DATE RANGE ===
const startDate = "01/08/2024"; // DD/MM/YYYY
const endDate = "31/08/2024"; // DD/MM/YYYY

// === CHUNK SIZE ===
const DATES_PER_CONFIG = 4;

// Track downloaded files to skip duplicates
const processed = new Set();

// ==================== GENERATE DATE RANGE ====================
function generateDateRange(start, end) {
  const dates = [];
  let current = new Date(start.split("/").reverse().join("-"));
  const endDateObj = new Date(end.split("/").reverse().join("-"));

  while (current <= endDateObj) {
    const day = String(current.getDate()).padStart(2, "0");
    const month = String(current.getMonth() + 1).padStart(2, "0");
    const year = current.getFullYear();
    dates.push(`${day}/${month}/${year}`);
    current.setDate(current.getDate() + 1);
  }
  return dates;
}

const allDates = generateDateRange(startDate, endDate);
console.log(
  `Total dates to process: ${allDates.length} (from ${startDate} to ${endDate})`
);

// ==================== AXIOS WITH RETRY ====================
async function fetchWithRetry(payload, configId, date, retries = 5) {
  for (let i = 0; i < retries; i++) {
    try {
      console.log(
        `Fetching configId=${configId}, date=${date} → Attempt ${i + 1}`
      );
      const response = await axios.post(url, payload, {
        headers: { "Content-Type": "application/json" },
        timeout: 0,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
      });

      if (response.data?.data || response.data) {
        console.log(`Success: configId=${configId}, ${date}`);
        return response.data;
      } else {
        console.warn(`Empty response for configId=${configId}, ${date}`);
        return null;
      }
    } catch (err) {
      console.warn(`Attempt ${i + 1} failed: ${err.message}`);
      if (i === retries - 1) throw err;
      await new Promise((r) => setTimeout(r, 7000 * (i + 1)));
    }
  }
}

// ==================== SAVE JSON FILE ====================
function saveJsonFile(configId, date, data, isError = false) {
  const safeDate = date.replace(/\//g, "-");
  const suffix = isError ? "_ERROR" : "";
  const filename = path.join(
    __dirname,
    `jansugam_${configId}_${safeDate}${suffix}.json`
  );
  fs.writeFileSync(filename, JSON.stringify(data, null, 2), "utf-8");
  const sizeMB = (JSON.stringify(data).length / 1024 / 1024).toFixed(2);
  console.log(
    `${isError ? "Error saved" : "Saved"}: ${filename} (${sizeMB} MB)`
  );
  return filename;
}

// ==================== MAIN DOWNLOADER ====================
async function downloadAll() {
  console.log(
    `\nJanSugam 4-Date Chunk Downloader Started @ ${new Date().toLocaleString(
      "en-IN"
    )}\n`
  );

  let totalDownloaded = 0;
  let dateIndex = 0;

  // Continue until all dates are assigned
  while (dateIndex < allDates.length) {
    for (const configId of configIds) {
      let chunkCount = 0;

      // Assign up to 4 dates to this configId
      while (chunkCount < DATES_PER_CONFIG && dateIndex < allDates.length) {
        const date = allDates[dateIndex];
        const cacheKey = `${configId}_${date}`;
        const cacheFile = path.join(
          __dirname,
          `jansugam_${configId}_${date.replace(/\//g, "-")}.json`
        );

        // Skip if already downloaded
        if (fs.existsSync(cacheFile)) {
          console.log(`Already exists: ${cacheFile} → Skipping`);
          dateIndex++;
          continue;
        }

        const payload = { configId, dataDate: date };

        try {
          const result = await fetchWithRetry(payload, configId, date);
          if (!result) {
            saveJsonFile(configId, date, { error: "No data returned" }, true);
            dateIndex++;
            chunkCount++;
            continue;
          }

          let parsedData;
          try {
            parsedData =
              typeof result.data === "string"
                ? JSON.parse(result.data)
                : result.data;
          } catch (parseErr) {
            console.log("Trying base64 decode...");
            try {
              const decoded = Buffer.from(result.data, "base64").toString(
                "utf-8"
              );
              parsedData = JSON.parse(decoded);
            } catch (decodeErr) {
              throw new Error("Parse failed: " + decodeErr.message);
            }
          }

          saveJsonFile(configId, date, parsedData);
          totalDownloaded++;
          chunkCount++;
          dateIndex++;

          // Polite delay
          await new Promise((r) => setTimeout(r, 2000));
        } catch (err) {
          console.error(`Failed: configId=${configId}, date=${date}`);
          saveJsonFile(
            configId,
            date,
            {
              error: err.message,
              timestamp: new Date().toISOString(),
              payload,
            },
            true
          );
          dateIndex++; // Don't retry same date infinitely
        }
      }

      if (dateIndex >= allDates.length) break;
    }
  }

  console.log(`\nAll done! Downloaded ${totalDownloaded} files.\n`);
}

// ==================== RUN ====================
downloadAll().catch((err) => {
  console.error("Downloader crashed:", err);
  process.exit(1);
});
