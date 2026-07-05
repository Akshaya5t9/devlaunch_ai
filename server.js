const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Middleware
app.use(cors());
app.use(express.json());

// Serve static files from the current directory
app.use(express.static(__dirname));

// Database file paths
const DATA_DIR = path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');

// Ensure data directory exists
try {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    console.log('📁 Data directory created');
  }
} catch (err) {
  console.error('Error creating data directory:', err);
}

// Initialize database
function initDB() {
  try {
    if (!fs.existsSync(DB_FILE)) {
      const defaultDB = {
        users: {},
        groups: {},
        activity_feed: [],
        notifications: {},
        personal: {}
      };
      fs.writeFileSync(DB_FILE, JSON.stringify(defaultDB, null, 2));
      console.log('📄 Database file created');
    }
    return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  } catch (err) {
    console.error('Error initializing database:', err);
    return {
      users: {},
      groups: {},
      activity_feed: [],
      notifications: {},
      personal: {}
    };
  }
}

// Save database
function saveDB(db) {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
  } catch (err) {
    console.error('Error saving database:', err);
  }
}

// Get database
function getDB() {
  try {
    if (!fs.existsSync(DB_FILE)) {
      return initDB();
    }
    return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  } catch (err) {
    console.error('Error reading database:', err);
    return initDB();
  }
}

// Initialize database on startup
initDB();

// WebSocket connections
const clients = new Map();

wss.on('connection', (ws, req) => {
  let userId = null;
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    userId = url.searchParams.get('userId');
    
    if (userId) {
      clients.set(userId, ws);
      console.log(`👤 User ${userId} connected (${clients.size} total)`);
      
      // Send any pending notifications
      const db = getDB();
      if (db.notifications && db.notifications[userId]) {
        const notifs = db.notifications[userId];
        if (notifs.length > 0) {
          ws.send(JSON.stringify({ 
            type: 'notifications', 
            data: notifs 
          }));
          // Clear sent notifications
          db.notifications[userId] = [];
          saveDB(db);
        }
      }
    } else {
      console.log('⚠️ WebSocket connection without userId');
    }
  } catch (err) {
    console.error('WebSocket connection error:', err);
  }

  ws.on('close', () => {
    if (userId) {
      clients.delete(userId);
      console.log(`👤 User ${userId} disconnected (${clients.size} total)`);
    }
  });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      if (data.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (e) {
      console.error('WebSocket message error:', e);
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
});

// Broadcast to all connected clients except sender
function broadcastNotification(userId, notification) {
  clients.forEach((ws, clientId) => {
    if (clientId !== userId && ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(JSON.stringify({
          type: 'notification',
          data: notification
        }));
      } catch (err) {
        console.error('Broadcast error:', err);
      }
    }
  });
}

// ============= API Routes =============

// Health check - important for Render
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    connections: clients.size,
    version: '1.0.0'
  });
});

// Get user data
app.get('/api/users/:email', (req, res) => {
  try {
    const db = getDB();
    const user = db.users[req.params.email];
    if (user) {
      res.json(user);
    } else {
      res.status(404).json({ error: 'User not found' });
    }
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Get all users
app.get('/api/users', (req, res) => {
  try {
    const db = getDB();
    res.json(db.users);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Create or update user
app.post('/api/users', (req, res) => {
  try {
    const db = getDB();
    const { email, name, password, isAdmin } = req.body;
    
    if (!email || !name || !password) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Check if user exists
    if (db.users[email]) {
      // Update existing user
      db.users[email] = { ...db.users[email], ...req.body };
    } else {
      // Create new user
      db.users[email] = {
        name,
        email,
        password,
        isAdmin: isAdmin || false,
        progressPercent: 0,
        quizScore: 0,
        groups: [],
        tasksDone: 0,
        lastActive: new Date().toISOString()
      };
    }
    
    saveDB(db);
    res.json(db.users[email]);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Get groups
app.get('/api/groups', (req, res) => {
  try {
    const db = getDB();
    res.json(db.groups);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Create group
app.post('/api/groups', (req, res) => {
  try {
    const db = getDB();
    const { id, name, idea, techStack, members, memberNames, visibility } = req.body;
    
    if (!id || !name || !members) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    db.groups[id] = {
      id,
      name,
      idea: idea || '',
      techStack: techStack || '',
      members,
      memberNames: memberNames || {},
      tasks: [],
      notes: [],
      resources: [],
      visibility: visibility || 'public',
      joinRequests: [],
      createdAt: new Date().toISOString()
    };
    
    saveDB(db);
    res.json(db.groups[id]);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Update group
app.put('/api/groups/:id', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    db.groups[groupId] = { ...db.groups[groupId], ...req.body };
    saveDB(db);
    
    // Broadcast update to group members
    const group = db.groups[groupId];
    (group.members || []).forEach(memberEmail => {
      if (clients.has(memberEmail)) {
        const ws = clients.get(memberEmail);
        if (ws.readyState === WebSocket.OPEN) {
          try {
            ws.send(JSON.stringify({
              type: 'group_update',
              data: { groupId, group }
            }));
          } catch (err) {
            console.error('Broadcast error:', err);
          }
        }
      }
    });
    
    res.json(db.groups[groupId]);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Get group by ID
app.get('/api/groups/:id', (req, res) => {
  try {
    const db = getDB();
    const group = db.groups[req.params.id];
    if (group) {
      res.json(group);
    } else {
      res.status(404).json({ error: 'Group not found' });
    }
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Join group
app.post('/api/groups/:id/join', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const { email, name } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    
    // Check if already member
    if (group.members.includes(email)) {
      return res.status(400).json({ error: 'Already a member' });
    }

    // Check if private group needs approval
    if (group.visibility === 'private') {
      const request = group.joinRequests.find(r => r.email === email);
      if (!request || request.status !== 'approved') {
        return res.status(403).json({ error: 'Join request not approved' });
      }
    }

    group.members.push(email);
    group.memberNames = group.memberNames || {};
    group.memberNames[email] = name;
    
    saveDB(db);
    
    // Notify other members
    group.members.forEach(memberEmail => {
      if (memberEmail !== email && clients.has(memberEmail)) {
        const ws = clients.get(memberEmail);
        if (ws.readyState === WebSocket.OPEN) {
          try {
            ws.send(JSON.stringify({
              type: 'member_joined',
              data: { groupId, member: { email, name } }
            }));
          } catch (err) {
            console.error('Broadcast error:', err);
          }
        }
      }
    });
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Request to join private group
app.post('/api/groups/:id/request', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const { email, name } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    
    if (group.members.includes(email)) {
      return res.status(400).json({ error: 'Already a member' });
    }

    group.joinRequests = group.joinRequests || [];
    if (group.joinRequests.some(r => r.email === email && r.status === 'pending')) {
      return res.status(400).json({ error: 'Request already pending' });
    }

    group.joinRequests.push({
      email,
      name,
      status: 'pending',
      ts: new Date().toISOString()
    });
    
    saveDB(db);
    
    // Notify group creator
    if (group.members.length > 0 && clients.has(group.members[0])) {
      const ws = clients.get(group.members[0]);
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(JSON.stringify({
            type: 'join_request',
            data: { groupId, request: { email, name } }
          }));
        } catch (err) {
          console.error('Broadcast error:', err);
        }
      }
    }
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Approve join request
app.post('/api/groups/:id/approve', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const { email } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    const request = group.joinRequests.find(r => r.email === email);
    
    if (!request) {
      return res.status(404).json({ error: 'Request not found' });
    }

    request.status = 'approved';
    saveDB(db);
    
    // Notify the user who was approved
    if (clients.has(email)) {
      const ws = clients.get(email);
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(JSON.stringify({
            type: 'request_approved',
            data: { groupId, groupName: group.name }
          }));
        } catch (err) {
          console.error('Broadcast error:', err);
        }
      }
    }
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Reject join request
app.post('/api/groups/:id/reject', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const { email } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    group.joinRequests = group.joinRequests.filter(r => r.email !== email);
    saveDB(db);
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Add task
app.post('/api/groups/:id/tasks', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const { task, assignedTo } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    group.tasks = group.tasks || [];
    const newTask = {
      id: 't_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      title: task,
      done: false,
      assignee: assignedTo || group.members[0],
      _notified: false,
      createdAt: new Date().toISOString()
    };
    group.tasks.push(newTask);
    
    saveDB(db);
    
    // Notify assigned user
    if (assignedTo && clients.has(assignedTo)) {
      const ws = clients.get(assignedTo);
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(JSON.stringify({
            type: 'task_assigned',
            data: { groupId, task: { title: task, assignedTo, groupName: group.name } }
          }));
        } catch (err) {
          console.error('Broadcast error:', err);
        }
      }
    }
    
    res.json({ success: true, task: newTask });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Toggle task status
app.patch('/api/groups/:id/tasks/:taskId', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const taskId = req.params.taskId;
    const { done } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    const task = group.tasks.find(t => t.id === taskId);
    
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    task.done = done;
    task._notified = true;
    saveDB(db);
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Add note
app.post('/api/groups/:id/notes', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const { author, text } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    group.notes = group.notes || [];
    const newNote = {
      author,
      text,
      ts: new Date().toISOString(),
      _read: false
    };
    group.notes.push(newNote);
    
    saveDB(db);
    
    // Notify all members
    group.members.forEach(memberEmail => {
      if (memberEmail !== author && clients.has(memberEmail)) {
        const ws = clients.get(memberEmail);
        if (ws.readyState === WebSocket.OPEN) {
          try {
            ws.send(JSON.stringify({
              type: 'new_note',
              data: { groupId, note: { author, text, groupName: group.name } }
            }));
          } catch (err) {
            console.error('Broadcast error:', err);
          }
        }
      }
    });
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Add resource
app.post('/api/groups/:id/resources', (req, res) => {
  try {
    const db = getDB();
    const groupId = req.params.id;
    const { title, url, note, addedBy } = req.body;
    
    if (!db.groups[groupId]) {
      return res.status(404).json({ error: 'Group not found' });
    }

    const group = db.groups[groupId];
    group.resources = group.resources || [];
    const newResource = {
      id: 'r_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
      title,
      url: url || '#',
      note: note || '',
      addedBy,
      ts: new Date().toISOString(),
      _read: false
    };
    group.resources.push(newResource);
    
    saveDB(db);
    
    // Notify all members
    group.members.forEach(memberEmail => {
      if (memberEmail !== addedBy && clients.has(memberEmail)) {
        const ws = clients.get(memberEmail);
        if (ws.readyState === WebSocket.OPEN) {
          try {
            ws.send(JSON.stringify({
              type: 'new_resource',
              data: { groupId, resource: { title, url, note, groupName: group.name } }
            }));
          } catch (err) {
            console.error('Broadcast error:', err);
          }
        }
      }
    });
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Get activity feed
app.get('/api/activity', (req, res) => {
  try {
    const db = getDB();
    res.json(db.activity_feed || []);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Add activity
app.post('/api/activity', (req, res) => {
  try {
    const db = getDB();
    const { actor, action } = req.body;
    db.activity_feed = db.activity_feed || [];
    db.activity_feed.unshift({ actor, action, ts: new Date().toISOString() });
    if (db.activity_feed.length > 60) db.activity_feed.length = 60;
    saveDB(db);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Get personal data
app.get('/api/personal/:email', (req, res) => {
  try {
    const db = getDB();
    const personal = db.personal[req.params.email] || {};
    res.json(personal);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Save personal data
app.post('/api/personal/:email', (req, res) => {
  try {
    const db = getDB();
    db.personal[req.params.email] = { ...db.personal[req.params.email], ...req.body };
    saveDB(db);
    res.json(db.personal[req.params.email]);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Get notifications for user
app.get('/api/notifications/:email', (req, res) => {
  try {
    const db = getDB();
    const notifs = db.notifications[req.params.email] || [];
    // Clear notifications after sending
    db.notifications[req.params.email] = [];
    saveDB(db);
    res.json(notifs);
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Add notification
app.post('/api/notifications/:email', (req, res) => {
  try {
    const db = getDB();
    const email = req.params.email;
    const notification = req.body;
    
    db.notifications[email] = db.notifications[email] || [];
    db.notifications[email].push(notification);
    saveDB(db);
    
    // Send via WebSocket if connected
    if (clients.has(email)) {
      const ws = clients.get(email);
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(JSON.stringify({
            type: 'notification',
            data: notification
          }));
        } catch (err) {
          console.error('Broadcast error:', err);
        }
      }
    }
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Server error' });
  }
});

// Root route - serve the HTML
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'devlaunch.html'));
});

// Catch-all route to serve the HTML for client-side routing
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'devlaunch.html'));
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📡 WebSocket server running on ws://0.0.0.0:${PORT}`);
  console.log(`🌐 Visit http://localhost:${PORT} to use DevLaunch`);
  console.log(`📊 Health check: http://localhost:${PORT}/api/health`);
});

// Handle graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, closing server...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});