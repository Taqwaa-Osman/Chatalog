/*
 * Chatalog Frontend
 * Connects chat UI to FastAPI backend
 */

let chatArea, messageInput, sendBtn, newChatBtn;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    chatArea = document.getElementById('chatArea');
    messageInput = document.getElementById('messageInput');
    sendBtn = document.getElementById('sendBtn');
    newChatBtn = document.getElementById('newChatBtn');
    
    sendBtn?.addEventListener('click', handleSend);
    messageInput?.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleSend();
    });
    newChatBtn?.addEventListener('click', () => {
        chatArea.innerHTML = '';
        messageInput.value = '';
    });
});

// Send message to API
async function handleSend() {
    const message = messageInput.value.trim();
    if (!message) return;
    
    // Show user message
    addMessage(message, 'user');
    messageInput.value = '';
    
    // Show loading with rotating messages
    sendBtn.disabled = true;
    const loadingWords = ['Thinking', 'Pondering', 'Caramelizing', 'Reflecting', 'Envisioning', 'Ruminating', 'Meditating', 'Picturing', 'Visualizing'];
    let wordIndex = 0;
    const loading = addMessage('Thinking...', 'assistant');
    const loadingInterval = setInterval(() => {
        wordIndex = (wordIndex + 1) % loadingWords.length;
        loading.querySelector('.message__content').textContent = loadingWords[wordIndex] + '...';
    }, 2000);
    
    try {
        const response = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message, limit: 5 })
        });
        
        const data = await response.json();
        clearInterval(loadingInterval);
        loading.remove();
        
        if (data.success) {
            addMessage(data.response, 'assistant', data.books);
        } else {
            addMessage('Sorry, something went wrong.', 'assistant');
        }
    } catch (error) {
        clearInterval(loadingInterval);
        loading.remove();
        addMessage('Could not connect to server.', 'assistant');
    }
    
    sendBtn.disabled = false;
}

// Add message to chat
function addMessage(text, role, books = null) {
    const div = document.createElement('div');
    div.className = `message message--${role}`;
    
    const content = document.createElement('div');
    content.className = 'message__content';
    content.innerHTML = text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\n/g, '<br>')
        .replace(/(https?:\/\/[^\s]+)/g, '<a href="$1" target="_blank">$1</a>');
    
    div.appendChild(content);
    
    // Add book cards if present
    if (books?.length) {
        const grid = document.createElement('div');
        grid.style.cssText = 'display:flex;flex-wrap:wrap;gap:10px;margin-top:15px;padding-top:15px;border-top:1px solid var(--border-light);';
        
        books.slice(0, 5).forEach(book => {
            const card = document.createElement('div');
            card.className = 'card card--light';
            card.style.cssText = 'flex:1;min-width:180px;max-width:250px;';
            card.innerHTML = `
                <div class="card__title">${book.title || 'Unknown'}</div>
                ${book.authors?.length ? `<div class="card__subtitle">by ${book.authors.join(', ')}</div>` : ''}
            `;
            grid.appendChild(card);
        });
        
        div.appendChild(grid);
    }
    
    chatArea.appendChild(div);
    chatArea.scrollTop = chatArea.scrollHeight;
    return div;
}