// services/json-processor.js - JSONL processing for customer account data
const fs = require('fs').promises;
const StreamValues = require('stream-json/streamers/StreamValues');
const parser = require('stream-json');
const _ = require('lodash');

class JSONProcessor {
  constructor() {
    this.stats = {
      totalRecords: 0,
      fieldsDetected: 0,
      fileSize: 0,
      format: 'unknown',
      sampleRecords: []
    };
  }

  // Flatten nested objects for BQ compatibility
  flattenObject(obj, prefix = '') {
    const flattened = {};
    
    for (const key in obj) {
      if (obj[key] === null || obj[key] === undefined) {
        flattened[prefix + key] = null;
      } else if (typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
        // Handle nested objects like u_user_criteria_record
        if (obj[key].value && obj[key].link) {
          // ServiceNow reference objects - extract value
          flattened[prefix + key + '_value'] = obj[key].value;
          flattened[prefix + key + '_link'] = obj[key].link;
        } else {
          // Other nested objects - flatten recursively
          Object.assign(flattened, this.flattenObject(obj[key], prefix + key + '_'));
        }
      } else if (Array.isArray(obj[key])) {
        // Convert arrays to JSON strings
        flattened[prefix + key] = JSON.stringify(obj[key]);
      } else {
        flattened[prefix + key] = obj[key];
      }
    }
    
    return flattened;
  }

  // Process JSONL file (line-by-line JSON)
  async processJSONL(filePath) {
    try {
      const content = await fs.readFile(filePath, 'utf8');
      const lines = content.trim().split('\n');
      const records = [];
      
      this.stats.format = 'JSONL';
      this.stats.totalRecords = lines.length;
      
      // Process each line as separate JSON object
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        try {
          const jsonObject = JSON.parse(line);
          const flattened = this.flattenObject(jsonObject);
          records.push(flattened);
          
          // Store first 3 records as samples
          if (i < 3) {
            this.stats.sampleRecords.push(flattened);
          }
        } catch (parseError) {
          console.error(`Error parsing line ${i + 1}:`, parseError.message);
        }
      }
      
      // Detect fields from first record
      if (records.length > 0) {
        this.stats.fieldsDetected = Object.keys(records[0]).length;
      }
      
      const fileStats = await fs.stat(filePath);
      this.stats.fileSize = fileStats.size;
      
      return {
        success: true,
        records: records,
        stats: this.stats
      };
      
    } catch (error) {
      console.error('JSONL processing error:', error);
      return {
        success: false,
        error: error.message,
        stats: this.stats
      };
    }
  }

  // Process regular JSON file (array of objects)
  async processJSON(filePath) {
    try {
      const content = await fs.readFile(filePath, 'utf8');
      const jsonData = JSON.parse(content);
      
      this.stats.format = 'JSON';
      
      let records = [];
      
      if (Array.isArray(jsonData)) {
        records = jsonData.map(obj => this.flattenObject(obj));
        this.stats.totalRecords = jsonData.length;
      } else {
        // Single object
        records = [this.flattenObject(jsonData)];
        this.stats.totalRecords = 1;
      }
      
      // Store first 3 records as samples
      this.stats.sampleRecords = records.slice(0, 3);
      
      // Detect fields from first record
      if (records.length > 0) {
        this.stats.fieldsDetected = Object.keys(records[0]).length;
      }
      
      const fileStats = await fs.stat(filePath);
      this.stats.fileSize = fileStats.size;
      
      return {
        success: true,
        records: records,
        stats: this.stats
      };
      
    } catch (error) {
      console.error('JSON processing error:', error);
      return {
        success: false,
        error: error.message,
        stats: this.stats
      };
    }
  }

  // Auto-detect and process file
  async processFile(filePath) {
    try {
      // Read first few lines to detect format
      const content = await fs.readFile(filePath, 'utf8');
      const firstLine = content.trim().split('\n')[0];
      
      // Try to parse first line as JSON
      try {
        JSON.parse(firstLine);
        const secondLine = content.trim().split('\n')[1];
        
        if (secondLine && secondLine.trim()) {
          try {
            JSON.parse(secondLine);
            // If both lines parse as JSON, it's likely JSONL
            console.log('Detected JSONL format');
            return await this.processJSONL(filePath);
          } catch {
            // First line is JSON, second isn't - probably regular JSON
            console.log('Detected JSON format');
            return await this.processJSON(filePath);
          }
        } else {
          // Only one line, try regular JSON first
          console.log('Single line detected, trying JSON format');
          return await this.processJSON(filePath);
        }
      } catch {
        return {
          success: false,
          error: 'Invalid JSON format',
          stats: this.stats
        };
      }
      
    } catch (error) {
      console.error('File processing error:', error);
      return {
        success: false,
        error: error.message,
        stats: this.stats
      };
    }
  }

  // Get file preview information
  async getFilePreview(filePath) {
    const result = await this.processFile(filePath);
    
    if (result.success) {
      return {
        success: true,
        preview: {
          totalRecords: this.stats.totalRecords,
          fieldsDetected: this.stats.fieldsDetected,
          fileSize: this.stats.fileSize,
          format: this.stats.format,
          sampleRecords: this.stats.sampleRecords,
          keyFields: this.getKeyFields(this.stats.sampleRecords[0] || {})
        }
      };
    }
    
    return result;
  }

  // Extract key fields for customer account data
  getKeyFields(record) {
    const keyFields = {};
    const importantFields = ['sys_id', 'number', 'name', 'u_tenant_id', 'u_account_type', 'u_status'];
    
    importantFields.forEach(field => {
      if (record.hasOwnProperty(field)) {
        keyFields[field] = record[field];
      }
    });
    
    return keyFields;
  }

  // Format file size for display
  formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
}

module.exports = JSONProcessor;