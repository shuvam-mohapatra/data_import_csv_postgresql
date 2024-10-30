const express = require("express");
const multer = require("multer");
const csv = require("fast-csv");
const { Pool } = require("pg");
const fs = require("fs");
require("dotenv").config();

const app = express();
const upload = multer({ dest: "uploads/" });
const format = require("pg-format");

// PostgreSQL connection configuration
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT || 5432,
});

// Function to sanitize column names for PostgreSQL
function sanitizeColumnName(columnName) {
  return columnName
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_")
    .replace(/^[0-9]/, "col_$&");
}

app.post("/import-csv", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded" });
  }

  const tableName = req.body.tableName;
  if (!tableName) {
    return res.status(400).json({ error: "Table name is required" });
  }

  try {
    const results = [];
    const fileRows = [];

    // Read CSV file
    await new Promise((resolve, reject) => {
      fs.createReadStream(req.file.path)
        .pipe(csv.parse({ headers: true }))
        .on("error", (error) => reject(error))
        .on("data", (row) => fileRows.push(row))
        .on("end", () => resolve());
    });

    if (fileRows.length === 0) {
      return res.status(400).json({ error: "CSV file is empty" });
    }

    // Get and sanitize column names
    const originalColumns = Object.keys(fileRows[0]);
    const columnMapping = {};
    const sanitizedColumns = originalColumns.map((col) => {
      const sanitized = sanitizeColumnName(col);
      columnMapping[col] = sanitized;
      return sanitized;
    });

    console.log("Column mapping:", columnMapping);

    // Drop existing table if it exists, using pg-format
    await pool.query(format("DROP TABLE IF EXISTS %I", tableName));

    // Create fresh table using pg-format
    const columnDefinitions = sanitizedColumns.map((col) => {
      if (col === "id") {
        return format("%I TEXT PRIMARY KEY", col);
      }
      return format("%I TEXT", col);
    });

    const createTableQuery = format(
      "CREATE TABLE %I (%s)",
      tableName,
      columnDefinitions.join(",\n")
    );

    await pool.query(createTableQuery);

    // Prepare and execute batch insert
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      for (const row of fileRows) {
        const insertColumns = Object.keys(row).map((col) => columnMapping[col]);
        const values = Object.keys(row).map((col) => row[col]);

        const insertQuery = format(
          `INSERT INTO %I (%I) VALUES (%L)`,
          tableName,
          insertColumns,
          values
        );

        await client.query(insertQuery);
      }

      await client.query("COMMIT");
      results.push(`Successfully imported ${fileRows.length} rows`);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }

    // Clean up uploaded file
    fs.unlinkSync(req.file.path);

    res.json({
      success: true,
      message: results.join("\n"),
      columnMapping,
    });
  } catch (error) {
    console.error("Error:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
