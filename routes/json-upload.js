// routes/json-upload.js - File upload handling for JSON vs BQ comparison
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const { v4: uuidv4 } = require('uuid');
const JSONProcessor = require('../services/json-processor');

const router = express.Router();

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'uploads/');
  },
  filename: function (req, file, cb) {
    const uniqueId = uuidv4();
    const extension = path.extname(file.originalname);
    cb(null, `${uniqueId}${extension}`);
  }
});

// File filter for JSON/JSONL files
const fileFilter = (req, file, cb) => {
  const allowedTypes = ['.json', '.jsonl'];
  const extension = path.extname(file.originalname).toLowerCase();
  
  if (allowedTypes.includes(extension)) {
    cb(null, true);
  } else {
    cb(new Error('Only JSON and JSONL files are allowed'), false);
  }
};

// Configure upload middleware
const upload = multer({
  storage: storage,
  fileFilter: fileFilter,
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB limit
  }
});

// POST /api/upload-json - Handle file upload
router.post('/upload-json', upload.single('jsonFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({
        success: false,
        error: 'No file uploaded'
      });
    }

    const fileInfo = {
      id: path.parse(req.file.filename).name,
      originalName: req.file.originalname,
      filename: req.file.filename,
      size: req.file.size,
      path: req.file.path,
      mimetype: req.file.mimetype,
      uploadedAt: new Date().toISOString()
    };

    // Basic file validation
    const fileSizeMB = (req.file.size / 1024 / 1024).toFixed(2);
    
    console.log(`File uploaded: ${req.file.originalname} (${fileSizeMB}MB)`);

    res.json({
      success: true,
      message: 'File uploaded successfully',
      file: fileInfo
    });

  } catch (error) {
    console.error('Upload error:', error);
    
    // Clean up file if error occurs
    if (req.file && req.file.path) {
      try {
        await fs.unlink(req.file.path);
      } catch (cleanupError) {
        console.error('Cleanup error:', cleanupError);
      }
    }

    res.status(500).json({
      success: false,
      error: error.message || 'File upload failed'
    });
  }
});

// GET /api/file-info/:id - Get file information
router.get('/file-info/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const filePath = path.join('uploads', `${id}.json`);
    const jsonlPath = path.join('uploads', `${id}.jsonl`);
    
    let targetPath = null;
    
    // Check for both .json and .jsonl extensions
    try {
      await fs.access(filePath);
      targetPath = filePath;
    } catch {
      try {
        await fs.access(jsonlPath);
        targetPath = jsonlPath;
      } catch {
        return res.status(404).json({
          success: false,
          error: 'File not found'
        });
      }
    }

    const stats = await fs.stat(targetPath);
    
    res.json({
      success: true,
      file: {
        id: id,
        path: targetPath,
        size: stats.size,
        created: stats.birthtime,
        modified: stats.mtime
      }
    });

  } catch (error) {
    console.error('File info error:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to get file information'
    });
  }
});

// GET /api/preview-json/:id - Preview uploaded JSON file
router.get('/preview-json/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const extensions = ['.json', '.jsonl'];
    let filePath = null;

    // Find the uploaded file
    for (const ext of extensions) {
      const testPath = path.join('uploads', `${id}${ext}`);
      try {
        await fs.access(testPath);
        filePath = testPath;
        break;
      } catch {
        // Continue to next extension
      }
    }

    if (!filePath) {
      return res.status(404).json({
        success: false,
        error: 'File not found'
      });
    }

    const processor = new JSONProcessor();
    const preview = await processor.getFilePreview(filePath);

    res.json(preview);

  } catch (error) {
    console.error('Preview error:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to preview file'
    });
  }
});

// DELETE /api/cleanup/:id - Clean up uploaded file
router.delete('/cleanup/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const extensions = ['.json', '.jsonl'];
    let deletedFiles = 0;

    for (const ext of extensions) {
      const filePath = path.join('uploads', `${id}${ext}`);
      try {
        await fs.unlink(filePath);
        deletedFiles++;
        console.log(`Cleaned up: ${filePath}`);
      } catch (error) {
        // File doesn't exist, continue
      }
    }

    if (deletedFiles > 0) {
      res.json({
        success: true,
        message: `Cleaned up ${deletedFiles} file(s)`
      });
    } else {
      res.status(404).json({
        success: false,
        error: 'No files found to clean up'
      });
    }

  } catch (error) {
    console.error('Cleanup error:', error);
    res.status(500).json({
      success: false,
      error: 'Cleanup failed'
    });
  }
});

module.exports = router;