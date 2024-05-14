// Imports
import fs from 'fs';
import os from 'os';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import oracledb from 'oracledb';

// Establishing S3 connection and Grabbing environment variables
const s3 = new S3Client();
const s3Bucket = process.env.S3_BUCKET_NAME;
const UserName = process.env.ORACLE_USERNAME;
const UserPassword = process.env.ORACLE_PASSWORD;
const ConnectionString = process.env.ORACLE_CONNECTION_STRING;

// Function to query the Oracle DB and write results to CSV
const queryAndWriteToCSV = async (username, user_password, hoststring, tableName) => {
    const csvFilePath = `${os.tmpdir()}/${tableName}_results.csv`;
    let connection;

    try {
        // Connection details
        const dbConfig = {
            user: username,
            password: user_password,
            connectString: hoststring
        };

        // Establish connection
        connection = await oracledb.getConnection(dbConfig);

        // Execute query
        const result = await connection.execute(`SELECT * FROM ${tableName}`);

        // Process query result
        const rows = result.rows;
        console.log(`Query Result for ${tableName}:`, rows);
        
        // Ensure /tmp directory exists
        if (!fs.existsSync('/tmp')) {
            fs.mkdirSync('/tmp');
        };

        // Write to CSV
        const csvData = rows.map(row => row.join(',')).join('\n');
        fs.writeFileSync(csvFilePath, csvData);
        console.log(`CSV file created at: ${csvFilePath}`); // success log message
        
        return csvFilePath;
    } catch (err) {
        console.error(`Error querying and writing to CSV for ${tableName}:`, err);
        throw err;
    } finally {
        // Release connection
        if (connection) {
            try {
                await connection.close();
            } catch (err) {
                console.error('Error closing connection:', err);
            }
        }
    }
};

// Function to upload a file to S3
const uploadFileToS3 = async (filePath, s3Key) => {
    const fileStream = fs.createReadStream(filePath);
    const params = {
        Bucket: s3Bucket,
        Key: s3Key,
        Body: fileStream
    };
    const put_command = new PutObjectCommand(params);

    try {
        const response = await s3.send(put_command);
        console.log("File uploaded successfully:", response);
        return response;
    } catch (error) {
        console.error("Error uploading file:", error);
        throw error;
    }
};

// Exporting AWS Lambda Handler "Wrapper" Function
export const handler = async (event, context) => {
    try {
        const tableNames = ['customers', 'people', 'organizations']; // List of tables to process
        
        const csvFilePaths = [];
        for (const tableName of tableNames) {
            const csvFilePath = await queryAndWriteToCSV(UserName, UserPassword, ConnectionString, tableName);
            csvFilePaths.push(csvFilePath);
        }

        // Upload all CSV files to S3
        for (const [index, csvFilePath] of csvFilePaths.entries()) {
            const s3ObjectKey = `table_${tableNames[index]}.csv`;
            await uploadFileToS3(csvFilePath, s3ObjectKey);
            console.log(`CSV file uploaded to S3 bucket: ${s3Bucket} with object key: ${s3ObjectKey}`);
        }

        return {
            statusCode: 200,
            body: 'Data exported successfully and uploaded to S3'
        };
    } catch (error) {
        console.error(error);
        return {
            statusCode: 500,
            body: `Error: ${error.message}`
        };
    }
};
