/**
 * Chatalog Authentication Module
 * Handles user login, registration, and session management
 */

const Auth = {
    // Current user state
    user: null,

    // DOM Elements
    elements: {},

    // Initialize auth module
    init() {
        this.cacheElements();
        this.bindEvents();
        this.checkSession();
    },

    // Cache DOM elements
    cacheElements() {
        this.elements = {
            // Auth modal
            authModal: document.getElementById('authModal'),
            modalBackdrop: document.getElementById('modalBackdrop'),
            modalClose: document.getElementById('modalClose'),
            
            // Login form
            loginForm: document.getElementById('loginForm'),
            loginEmail: document.getElementById('loginEmail'),
            loginPassword: document.getElementById('loginPassword'),
            loginSubmit: document.getElementById('loginSubmit'),
            loginError: document.getElementById('loginError'),
            showRegister: document.getElementById('showRegister'),
            
            // Register form
            registerForm: document.getElementById('registerForm'),
            registerName: document.getElementById('registerName'),
            registerEmail: document.getElementById('registerEmail'),
            registerPassword: document.getElementById('registerPassword'),
            registerCard: document.getElementById('registerCard'),
            registerSubmit: document.getElementById('registerSubmit'),
            registerError: document.getElementById('registerError'),
            showLogin: document.getElementById('showLogin'),
            
            // Header elements
            loginBtn: document.getElementById('loginBtn'),
            userMenu: document.getElementById('userMenu'),
            userMenuBtn: document.getElementById('userMenuBtn'),
            userDropdown: document.getElementById('userDropdown'),
            userAvatar: document.getElementById('userAvatar'),
            usernameDisplay: document.getElementById('usernameDisplay'),
            logoutBtn: document.getElementById('logoutBtn'),
            myChatsBtn: document.getElementById('myChatsBtn')
        };
    },

    // Bind event listeners
    bindEvents() {
        const e = this.elements;
        
        // Open auth modal
        e.loginBtn?.addEventListener('click', (ev) => {
            ev.preventDefault();
            this.openModal();
        });
        
        // Close modal
        e.modalBackdrop?.addEventListener('click', () => this.closeModal());
        e.modalClose?.addEventListener('click', () => this.closeModal());
        
        // Switch forms
        e.showRegister?.addEventListener('click', (ev) => {
            ev.preventDefault();
            this.showForm('register');
        });
        
        e.showLogin?.addEventListener('click', (ev) => {
            ev.preventDefault();
            this.showForm('login');
        });
        
        // Submit forms
        e.loginSubmit?.addEventListener('click', () => this.login());
        e.registerSubmit?.addEventListener('click', () => this.register());
        
        // Enter key submission
        e.loginPassword?.addEventListener('keypress', (ev) => {
            if (ev.key === 'Enter') this.login();
        });
        e.registerCard?.addEventListener('keypress', (ev) => {
            if (ev.key === 'Enter') this.register();
        });
        
        // User menu toggle
        e.userMenuBtn?.addEventListener('click', () => this.toggleDropdown());
        
        // Close dropdown when clicking outside
        document.addEventListener('click', (ev) => {
            if (!e.userMenu?.contains(ev.target)) {
                e.userDropdown?.classList.add('hidden');
            }
        });
        
        // Logout
        e.logoutBtn?.addEventListener('click', (ev) => {
            ev.preventDefault();
            this.logout();
        });
        
        // My chats
        e.myChatsBtn?.addEventListener('click', (ev) => {
            ev.preventDefault();
            e.userDropdown?.classList.add('hidden');
            this.loadUserChats();
        });
        
        // Escape key closes modal
        document.addEventListener('keydown', (ev) => {
            if (ev.key === 'Escape') {
                this.closeModal();
            }
        });
    },

    // Check for existing session
    checkSession() {
        const stored = localStorage.getItem('chatalog_user');
        if (stored) {
            try {
                this.user = JSON.parse(stored);
                this.updateUI();
            } catch (e) {
                localStorage.removeItem('chatalog_user');
            }
        }
    },

    // Open auth modal
    openModal() {
        this.elements.authModal?.classList.remove('hidden');
        this.showForm('login');
        this.elements.loginEmail?.focus();
    },

    // Close auth modal
    closeModal() {
        this.elements.authModal?.classList.add('hidden');
        this.clearForms();
    },

    // Show login or register form
    showForm(type) {
        const e = this.elements;
        if (type === 'login') {
            e.loginForm?.classList.remove('hidden');
            e.registerForm?.classList.add('hidden');
            e.loginEmail?.focus();
        } else {
            e.loginForm?.classList.add('hidden');
            e.registerForm?.classList.remove('hidden');
            e.registerName?.focus();
        }
        this.clearErrors();
    },

    // Clear form inputs
    clearForms() {
        const e = this.elements;
        if (e.loginEmail) e.loginEmail.value = '';
        if (e.loginPassword) e.loginPassword.value = '';
        if (e.registerName) e.registerName.value = '';
        if (e.registerEmail) e.registerEmail.value = '';
        if (e.registerPassword) e.registerPassword.value = '';
        if (e.registerCard) e.registerCard.value = '';
        this.clearErrors();
    },

    // Clear error messages
    clearErrors() {
        this.elements.loginError?.classList.add('hidden');
        this.elements.registerError?.classList.add('hidden');
    },

    // Show error message
    showError(type, message) {
        const el = type === 'login' ? this.elements.loginError : this.elements.registerError;
        if (el) {
            el.textContent = message;
            el.classList.remove('hidden');
        }
    },

    // Login user
    async login() {
        const e = this.elements;
        const email = e.loginEmail?.value.trim();
        const password = e.loginPassword?.value;
        
        if (!email || !password) {
            this.showError('login', 'Please enter email and password');
            return;
        }
        
        e.loginSubmit.disabled = true;
        e.loginSubmit.textContent = 'Signing in...';
        
        try {
            const res = await fetch('/api/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email, password })
            });
            
            const data = await res.json();
            
            if (!res.ok) {
                throw new Error(data.detail || 'Login failed');
            }
            
            this.user = {
                user_id: data.user_id,
                username: data.username,
                email: data.email,
                library_card: data.library_card
            };
            
            localStorage.setItem('chatalog_user', JSON.stringify(this.user));
            this.updateUI();
            this.closeModal();
            
            // Reload chat history for this user
            if (typeof loadChatHistory === 'function') {
                loadChatHistory();
            }
            
        } catch (err) {
            this.showError('login', err.message);
        } finally {
            e.loginSubmit.disabled = false;
            e.loginSubmit.textContent = 'Sign In';
        }
    },

    // Register new user
    async register() {
        const e = this.elements;
        const username = e.registerName?.value.trim();
        const email = e.registerEmail?.value.trim();
        const password = e.registerPassword?.value;
        const library_card = e.registerCard?.value.trim();
        
        if (!username || !email || !password) {
            this.showError('register', 'Please fill in all required fields');
            return;
        }
        
        if (password.length < 6) {
            this.showError('register', 'Password must be at least 6 characters');
            return;
        }
        
        e.registerSubmit.disabled = true;
        e.registerSubmit.textContent = 'Creating account...';
        
        try {
            const res = await fetch('/api/auth/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, email, password, library_card })
            });
            
            const data = await res.json();
            
            if (!res.ok) {
                throw new Error(data.detail || 'Registration failed');
            }
            
            this.user = {
                user_id: data.user_id,
                username: data.username,
                email: data.email,
                library_card: data.library_card
            };
            
            localStorage.setItem('chatalog_user', JSON.stringify(this.user));
            this.updateUI();
            this.closeModal();
            
            // Reload chat history for this user
            if (typeof loadChatHistory === 'function') {
                loadChatHistory();
            }
            
        } catch (err) {
            this.showError('register', err.message);
        } finally {
            e.registerSubmit.disabled = false;
            e.registerSubmit.textContent = 'Create Account';
        }
    },

    // Logout user
    logout() {
        this.user = null;
        localStorage.removeItem('chatalog_user');
        this.updateUI();
        this.elements.userDropdown?.classList.add('hidden');
        
        // Reload chat history (will show anonymous chats)
        if (typeof loadChatHistory === 'function') {
            loadChatHistory();
        }
    },

    // Update UI based on auth state
    updateUI() {
        const e = this.elements;
        
        if (this.user) {
            // Show user menu, hide login button
            e.loginBtn?.classList.add('hidden');
            e.userMenu?.classList.remove('hidden');
            
            // Update user info
            if (e.userAvatar) {
                e.userAvatar.textContent = this.user.username.charAt(0).toUpperCase();
            }
            if (e.usernameDisplay) {
                e.usernameDisplay.textContent = this.user.username;
            }
        } else {
            // Show login button, hide user menu
            e.loginBtn?.classList.remove('hidden');
            e.userMenu?.classList.add('hidden');
        }
    },

    // Toggle user dropdown
    toggleDropdown() {
        this.elements.userDropdown?.classList.toggle('hidden');
    },

    // Load user's chat history
    async loadUserChats() {
        if (!this.user) return;
        
        try {
            const res = await fetch(`/api/sessions/user/${this.user.user_id}`);
            const sessions = await res.json();
            
            // Update the chat history sidebar
            const chatHistory = document.getElementById('chatHistory');
            if (chatHistory && sessions.length > 0) {
                chatHistory.innerHTML = sessions.map(s => `
                    <li class="sidebar__chat-item" data-session="${s.session_id}">
                        ${this.escapeHtml(s.title || 'New Chat')}
                    </li>
                `).join('');
            }
        } catch (err) {
            console.error('Failed to load user chats:', err);
        }
    },

    // Helper: Get current user ID for API calls
    getUserId() {
        return this.user?.user_id || null;
    },

    // Helper: Check if user is logged in
    isLoggedIn() {
        return !!this.user;
    },

    // Helper: Escape HTML
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    Auth.init();
});

// Make Auth available globally
window.Auth = Auth;