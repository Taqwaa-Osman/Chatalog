/*
 * Chatalog Frontend
 */

console.log('app.js loaded');
let chatArea, messageInput, sendBtn, newChatBtn, chatHistoryList;
let currentSessionId = null;

document.addEventListener('DOMContentLoaded', () => {
    chatArea = document.getElementById('chatArea');
    messageInput = document.getElementById('messageInput');
    sendBtn = document.getElementById('sendBtn');
    newChatBtn = document.getElementById('newChatBtn');
    chatHistoryList = document.getElementById('chatHistory');
    
    sendBtn?.addEventListener('click', handleSend);
    messageInput?.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleSend();
    });
    newChatBtn?.addEventListener('click', startNewChat);
    
    // Chat history is loaded by auth.js after checkSession() completes.
    // This avoids the race condition where Auth.user is still null at this point.
});

async function loadChatHistory() {
    // Only show chat history if user is logged in
    if (!window.Auth?.isLoggedIn()) {
        renderChatHistory([]);
        return;
    }
    
    try {
        const url = `/api/sessions/user/${Auth.getUserId()}?limit=20`;
        const response = await fetch(url);
        const sessions = await response.json();
        renderChatHistory(sessions);
    } catch (error) {
        console.error('Failed to load history:', error);
        renderChatHistory([]);
    }
}

function renderChatHistory(sessions) {
    if (!chatHistoryList) return;
    chatHistoryList.innerHTML = '';
    
    // If not logged in, show sign-in prompt
    if (!window.Auth?.isLoggedIn()) {
        chatHistoryList.innerHTML = `
            <li class="sidebar__chat-item text-light" style="text-align:center;padding:20px 15px;">
                <div style="margin-bottom:8px;">💬</div>
                <div style="font-size:13px;">Sign in to save your chat history</div>
            </li>
        `;
        return;
    }
    
    if (sessions.length === 0) {
        chatHistoryList.innerHTML = '<li class="sidebar__chat-item text-light">No past chats yet</li>';
        return;
    }
    
    sessions.forEach(session => {
        const li = document.createElement('li');
        li.className = 'sidebar__chat-item';
        if (session.session_id === currentSessionId) {
            li.classList.add('sidebar__chat-item--active');
        }
        
        const title = session.title || 'New Chat';
        const displayTitle = title.length > 35 ? title.substring(0, 35) + '...' : title;
        
        const date = new Date(session.updated_at);
        const timeAgo = getTimeAgo(date);
        
        li.innerHTML = `
            <div style="font-size:14px;">${escapeHtml(displayTitle)}</div>
            <div style="font-size:11px;color:var(--text-light);margin-top:2px;">${timeAgo}</div>
        `;
        
        li.addEventListener('click', () => loadSession(session.session_id));
        chatHistoryList.appendChild(li);
    });
}

async function loadSession(sessionId) {
    try {
        const response = await fetch(`/api/sessions/${sessionId}/messages`);
        const messages = await response.json();
        
        chatArea.innerHTML = '';
        currentSessionId = sessionId;
        
        messages.forEach(msg => {
            const books = msg.books?.map(title => ({ title })) || [];
            addMessage(msg.content, msg.role, books, false);
        });
        
        loadChatHistory();
        chatArea.scrollTop = chatArea.scrollHeight;
    } catch (error) {
        console.error('Failed to load session:', error);
    }
}

function startNewChat() {
    currentSessionId = null;
    chatArea.innerHTML = '';
    messageInput.value = '';
    messageInput.focus();
    
    // Only reload history if logged in
    if (window.Auth?.isLoggedIn()) {
        loadChatHistory();
    }
}

async function handleSend() {
    const message = messageInput.value.trim();
    if (!message) return;
    
    addMessage(message, 'user');
    messageInput.value = '';
    
    sendBtn.disabled = true;
    const loadingWords = ['Thinking', 'Pondering', 'Caramelizing', 'Reflecting', 'Envisioning', 'Ruminating', 'Meditating', 'Picturing', 'Visualizing'];
    let wordIndex = 0;
    const loading = addMessage('Thinking...', 'assistant');
    const loadingInterval = setInterval(() => {
        wordIndex = (wordIndex + 1) % loadingWords.length;
        loading.querySelector('.message__content').textContent = loadingWords[wordIndex] + '...';
    }, 2000);
    
    try {
        const payload = { 
            message, 
            session_id: currentSessionId, 
            limit: 5 
        };
        
        // Include user_id if logged in
        if (window.Auth?.isLoggedIn()) {
            payload.user_id = Auth.getUserId();
        }
        
        const response = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        const data = await response.json();
        clearInterval(loadingInterval);
        loading.remove();
        
        if (data.success) {
            currentSessionId = data.session_id;
            addMessage(data.response, 'assistant', data.books);
            loadChatHistory();
        } else {
            addMessage('Sorry, something went wrong.', 'assistant');
        }
    } catch (error) {
        clearInterval(loadingInterval);
        loading.remove();
        addMessage('Could not connect to server.', 'assistant');
    }
    
    sendBtn.disabled = false;
    messageInput.focus();
}

function addMessage(text, role, books = null, animate = true) {
    const div = document.createElement('div');
    div.className = `message message--${role}`;
    if (!animate) div.style.animation = 'none';
    
    const content = document.createElement('div');
    content.className = 'message__content';
    content.innerHTML = escapeHtml(text)
        .replace(/\n/g, '<br>')
        .replace(/(https?:\/\/[^\s]+)/g, '<a href="$1" target="_blank">$1</a>');
    
    div.appendChild(content);
    
    if (books?.length) {
        const grid = document.createElement('div');
        grid.style.cssText = 'display:flex;flex-wrap:wrap;gap:10px;margin-top:15px;padding-top:15px;border-top:1px solid var(--border-light);';
        
        books.slice(0, 5).forEach(book => {
            const card = document.createElement('div');
            card.className = 'card card--light';
            card.style.cssText = 'flex:1;min-width:180px;max-width:250px;';
            card.innerHTML = `
                <div class="card__title">${escapeHtml(book.title || 'Unknown')}</div>
                ${book.authors?.length ? `<div class="card__subtitle">by ${escapeHtml(book.authors.join(', '))}</div>` : ''}
            `;
            grid.appendChild(card);
        });
        
        div.appendChild(grid);
    }
    
    chatArea.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
    return div;
}

function escapeHtml(text) {
    if (!text) return '';
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function getTimeAgo(date) {
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
}

// Export for auth.js to use
window.loadChatHistory = loadChatHistory;