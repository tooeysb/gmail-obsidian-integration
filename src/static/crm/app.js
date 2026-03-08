function crmApp() {
    return {
        // ==================== STATE ====================
        currentView: 'dashboard',
        searchQuery: '',
        searchResults: null,
        showSearchResults: false,

        // Dashboard
        dashboard: { loading: true, data: null },
        emailVolumeChart: null,

        // Contacts
        contacts: {
            loading: true,
            items: [],
            total: 0,
            page: 1,
            pageSize: 50,
            totalPages: 0,
            search: '',
            sortBy: 'email_count',
            sortDir: 'desc',
            filters: { is_vip: null, contact_type: '', tags: '', company_id: '' },
        },

        // Companies
        _editingCompanyId: null,
        companies: {
            loading: true,
            items: [],
            total: 0,
            page: 1,
            pageSize: 50,
            totalPages: 0,
            search: '',
            sortBy: 'arr',
            sortDir: 'desc',
            filters: { company_type: '', account_tier: '', enr: '', contact_status: 'contacted' },
        },

        // LinkedIn
        linkedin: {
            loading: true,
            tab: 'posts',
            posts: [],
            postsTotal: 0,
            jobChanges: [],
            titleChanges: [],
            summary: null,
            tiers: { loading: false },
        },

        // Outreach
        outreach: {
            loading: true,
            stats: {},
            items: [],
            total: 0,
            page: 1,
            pageSize: 20,
            totalPages: 0,
            filter: 'pending',
            editingId: null,
            editSubject: '',
            editBody: '',
            pipelineRunning: false,
            tab: 'drafts',
            news: [],
            newsTotal: 0,
            newsPage: 1,
            newsTotalPages: 0,
            newsLoading: false,
            newsFilter: 'all',
        },

        // Reports
        reports: {
            loading: false,
            selected: localStorage.getItem('crm_report_tab') || 'needsLinkedIn',
            companiesWithoutPeople: [],
            needsLinkedIn: [],
            needsBrowserEnrich: [],
            needsHumanResearch: [],
            jobChanges: [],
            needsLeadership: [],
        },

        // Admin
        admin: { selected: ['reports', 'enhancements'].includes(localStorage.getItem('crm_admin_tab')) ? localStorage.getItem('crm_admin_tab') : 'reports' },

        // Detail panel
        detail: {
            show: false,
            type: null,
            id: null,
            loading: true,
            data: null,
        },
        _savedScrollY: 0,
        _returnTo: null, // breadcrumb: {view, adminTab, reportTab} to return to after detail

        // Detail emails pagination
        emails: [],
        emailsPage: 1,
        emailsTotal: 0,
        emailsLoading: false,

        // Inline editing
        editing: { field: null, value: '' },
        newTag: '',

        // Debounce timers
        _notesTimer: null,

        // ==================== LIFECYCLE ====================
        init() {
            window.addEventListener('hashchange', () => this._restoreFromHash());
            this._restoreFromHash();
            this.$watch('reports.selected', (val) => localStorage.setItem('crm_report_tab', val));
            this.$watch('admin.selected', (val) => localStorage.setItem('crm_admin_tab', val));
            // Fetch LinkedIn post count for nav badge
            this.apiFetch('reports/contact-activity-summary').then(data => {
                if (data) this.linkedin.postsTotal = data.new_posts || 0;
            });
        },

        // ==================== NAVIGATION ====================
        navigate(view) {
            this.currentView = view;
            this.closeDetail();
            window.location.hash = view;
            if (view === 'dashboard' && !this.dashboard.data) {
                this.loadDashboard();
            } else if (view === 'contacts' && this.contacts.items.length === 0) {
                this.loadContacts();
            } else if (view === 'companies' && this.companies.items.length === 0) {
                this.loadCompanies();
            } else if (view === 'outreach') {
                this.loadOutreach();
            } else if (view === 'linkedin') {
                this.loadLinkedIn();
            } else if (view === 'admin') {
                if (this.admin.selected === 'reports') this.loadReports();
            }
        },

        _restoreFromHash() {
            const hash = window.location.hash.replace('#', '');
            if (!hash) {
                this.loadDashboard();
                return;
            }
            const [view, id] = hash.split('/');
            // Backward compat: old #reports and #noContact redirect to #admin
            if (view === 'reports' || view === 'noContact') {
                if (view === 'noContact') {
                    // Redirect old noContact to companies with uncontacted filter
                    this.companies.filters.contact_status = 'uncontacted';
                    this.currentView = 'companies';
                    window.location.hash = 'companies';
                    this.loadCompanies();
                    return;
                }
                this.admin.selected = 'reports';
                this.currentView = 'admin';
                window.location.hash = 'admin';
                this.loadReports();
                return;
            }
            if (['dashboard', 'contacts', 'companies', 'outreach', 'linkedin', 'admin'].includes(view)) {
                this.currentView = view;
                if (view === 'dashboard') this.loadDashboard();
                else if (view === 'contacts') this.loadContacts();
                else if (view === 'companies') this.loadCompanies();
                else if (view === 'outreach') this.loadOutreach();
                else if (view === 'linkedin') this.loadLinkedIn();
                else if (view === 'admin') { this.loadReports(); }

                if (id) {
                    if (view === 'companies') this.openCompanyDetail(id);
                    else if (view === 'contacts') this.openContactDetail(id);
                }
            } else {
                this.loadDashboard();
            }
        },

        // ==================== API HELPERS ====================
        async apiFetch(path, options = {}) {
            try {
                const headers = { 'Content-Type': 'application/json', ...options.headers };
                if (window.CRM_API_KEY) headers['X-API-Key'] = window.CRM_API_KEY;
                const { headers: _h, ...restOptions } = options;
                const resp = await fetch('/crm/api/' + path, {
                    headers,
                    ...restOptions,
                });
                if (!resp.ok) throw new Error(`API error: ${resp.status}`);
                return await resp.json();
            } catch (err) {
                console.error('API error:', err);
                return null;
            }
        },

        // ==================== DASHBOARD ====================
        async loadDashboard() {
            this.dashboard.loading = true;
            const data = await this.apiFetch('dashboard');
            if (data) {
                this.dashboard.data = data;
                this.$nextTick(() => this.renderEmailChart());
            }
            this.dashboard.loading = false;
        },

        renderEmailChart() {
            const canvas = document.getElementById('emailVolumeChart');
            if (!canvas) return;

            if (this.emailVolumeChart) {
                this.emailVolumeChart.destroy();
            }

            const volumeData = [...(this.dashboard.data?.email_volume_by_month || [])].reverse();
            const labels = volumeData.map((d) => {
                const [y, m] = d.month.split('-');
                const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                return months[parseInt(m) - 1] + ' ' + y.slice(2);
            });
            const values = volumeData.map((d) => d.count);

            this.emailVolumeChart = new Chart(canvas, {
                type: 'bar',
                data: {
                    labels,
                    datasets: [{
                        label: 'Emails',
                        data: values,
                        backgroundColor: '#818CF8',
                        borderRadius: 4,
                        borderSkipped: false,
                        maxBarThickness: 40,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: '#1F2937',
                            titleFont: { size: 12 },
                            bodyFont: { size: 12 },
                            padding: 10,
                            cornerRadius: 8,
                            callbacks: {
                                label: (ctx) => ctx.parsed.y.toLocaleString() + ' emails',
                            },
                        },
                    },
                    scales: {
                        x: {
                            grid: { display: false },
                            ticks: { font: { size: 11 }, color: '#9CA3AF' },
                        },
                        y: {
                            grid: { color: '#F3F4F6' },
                            ticks: {
                                font: { size: 11 },
                                color: '#9CA3AF',
                                callback: (v) => v >= 1000 ? (v / 1000).toFixed(0) + 'K' : v,
                            },
                            beginAtZero: true,
                        },
                    },
                },
            });
        },

        // ==================== CONTACTS ====================
        async loadContacts() {
            this.contacts.loading = true;
            const params = new URLSearchParams({
                page: this.contacts.page,
                page_size: this.contacts.pageSize,
                sort_by: this.contacts.sortBy,
                sort_dir: this.contacts.sortDir,
            });
            if (this.contacts.search) params.set('search', this.contacts.search);
            if (this.contacts.filters.is_vip) params.set('is_vip', 'true');
            if (this.contacts.filters.contact_type) params.set('contact_type', this.contacts.filters.contact_type);
            if (this.contacts.filters.tags) params.set('tags', this.contacts.filters.tags);
            if (this.contacts.filters.company_id) params.set('company_id', this.contacts.filters.company_id);

            const data = await this.apiFetch('contacts?' + params.toString());
            if (data) {
                this.contacts.items = data.items || [];
                this.contacts.total = data.total || 0;
                this.contacts.totalPages = data.total_pages || 0;
            }
            this.contacts.loading = false;
        },

        // ==================== COMPANIES ====================
        async loadCompanies() {
            this.companies.loading = true;
            const params = new URLSearchParams({
                page: this.companies.page,
                page_size: this.companies.pageSize,
                sort_by: this.companies.sortBy,
                sort_dir: this.companies.sortDir,
            });
            if (this.companies.search) {
                params.set('search', this.companies.search);
                params.set('contact_filter', 'all');
            } else {
                params.set('contact_filter', this.companies.filters.contact_status || 'contacted');
            }
            if (this.companies.filters.company_type) params.set('company_type', this.companies.filters.company_type);
            if (this.companies.filters.account_tier) params.set('account_tier', this.companies.filters.account_tier);
            if (this.companies.filters.enr) params.set('enr', this.companies.filters.enr);

            const data = await this.apiFetch('companies?' + params.toString());
            if (data) {
                this.companies.items = data.items || [];
                this.companies.total = data.total || 0;
                this.companies.totalPages = data.total_pages || 0;
            }
            this.companies.loading = false;
        },

        // ==================== DETAIL PANEL ====================
        async openContactDetail(id) {
            this._savedScrollY = window.scrollY;
            if (!this._returnTo) {
                this._returnTo = this.currentView === 'admin'
                    ? { view: 'admin', adminTab: this.admin.selected, reportTab: this.reports.selected }
                    : null;
            }
            this.detail = { show: true, type: 'contact', id, loading: true, data: null, enrichingTitle: false };
            this.emails = [];
            this.emailsPage = 1;
            this.emailsTotal = 0;
            this.editing = { field: null, value: '' };
            this.newTag = '';
            window.location.hash = 'contacts/' + id;

            const data = await this.apiFetch('contacts/' + id);
            if (data) {
                this.detail.data = data;
                this.detail.emails = data.recent_emails || [];
                this.detail.emailsTotal = data.email_stats?.total || 0;
                // Load paginated emails
                this.loadContactEmails(id, true);

                // Async title enrichment if title is missing
                if (!data.contact?.title) {
                    this.detail.enrichingTitle = true;
                    this.apiFetch('contacts/' + id + '/enrich-title', { method: 'POST' })
                        .then(result => {
                            if (result?.title && this.detail.id === id) {
                                this.detail.data.contact.title = result.title;
                            }
                        })
                        .catch(() => {})
                        .finally(() => { this.detail.enrichingTitle = false; });
                }
            }
            this.detail.loading = false;
        },

        async openCompanyDetail(id) {
            this._savedScrollY = window.scrollY;
            if (!this._returnTo) {
                this._returnTo = this.currentView === 'admin'
                    ? { view: 'admin', adminTab: this.admin.selected, reportTab: this.reports.selected }
                    : null;
            }
            this.detail = { show: true, type: 'company', id, loading: true, data: null, recentNews: [], olderNews: [], newsLoading: false, newsTotal: 0, companyTab: 'overview', discoveredContacts: [], discoveredLoading: false, discoveredDomain: '', scanning: false, scanMessage: '', mergeMode: false, mergeSearch: '', mergeResults: [], merging: false, showInactive: false };
            this.editing = { field: null, value: '' };
            window.location.hash = 'companies/' + id;

            const data = await this.apiFetch('companies/' + id);
            if (data) {
                this.detail.data = data;
                this.loadCompanyNews(id);
            }
            this.detail.loading = false;
        },

        async loadCompanyNews(companyId) {
            this.detail.newsLoading = true;
            const data = await this.apiFetch('outreach/news?company_id=' + companyId + '&page_size=50&sort_by=published_at&sort_dir=desc');
            if (data) {
                const items = (data.items || []).map(i => {
                    // Strip HTML tags from summary
                    if (i.summary) i.summary = i.summary.replace(/<[^>]*>/g, '').trim();
                    return i;
                });
                const cutoff = Date.now() - 24 * 60 * 60 * 1000;
                this.detail.recentNews = items.filter(i => {
                    return i.published_at && new Date(i.published_at).getTime() >= cutoff;
                });
                this.detail.olderNews = items.filter(i => {
                    return !i.published_at || new Date(i.published_at).getTime() < cutoff;
                });
                this.detail.newsTotal = data.total || 0;
            }
            this.detail.newsLoading = false;
        },

        async loadDiscoveredContacts(companyId) {
            if (this.detail.discoveredContacts.length > 0) return; // already loaded
            this.detail.discoveredLoading = true;
            const data = await this.apiFetch('companies/' + companyId + '/discovered-contacts');
            if (data) {
                this.detail.discoveredContacts = (data.discovered || []).map(p => ({ ...p, adding: false, added: false }));
                this.detail.discoveredDomain = data.domain || '';
            }
            this.detail.discoveredLoading = false;
        },

        async addDiscoveredContact(companyId, person, idx) {
            this.detail.discoveredContacts[idx].adding = true;
            const result = await this.apiFetch('companies/' + companyId + '/contacts', {
                method: 'POST',
                body: JSON.stringify({ email: person.email, name: person.name, title: person.title || null }),
            });
            if (result) {
                this.detail.discoveredContacts[idx].added = true;
                // Refresh the company detail to show updated contact list
                const data = await this.apiFetch('companies/' + companyId);
                if (data) this.detail.data = data;
            }
            this.detail.discoveredContacts[idx].adding = false;
        },

        closeDetail() {
            this.detail.show = false;
            this.editing = { field: null, value: '' };
            if (this._returnTo) {
                this.currentView = this._returnTo.view;
                if (this._returnTo.adminTab) this.admin.selected = this._returnTo.adminTab;
                if (this._returnTo.reportTab) this.reports.selected = this._returnTo.reportTab;
                window.location.hash = this._returnTo.view;
                this._returnTo = null;
            } else {
                window.location.hash = this.currentView;
            }
            this.$nextTick(() => { window.scrollTo(0, this._savedScrollY); });
        },

        async deleteCompany(id) {
            const name = this.detail.data?.company?.name || 'this company';
            if (!confirm(`Delete "${name}" and all its contacts? This cannot be undone.`)) return;
            const result = await this.apiFetch('companies/' + id, { method: 'DELETE' });
            if (result) {
                this.closeDetail();
                this.loadCompanies();
            }
        },

        async deleteContact(id) {
            const name = this.detail.data?.name || this.detail.data?.email || 'this contact';
            if (!confirm(`Delete "${name}"? This cannot be undone.`)) return;
            const result = await this.apiFetch('contacts/' + id, { method: 'DELETE' });
            if (result) {
                this.closeDetail();
                this.loadContacts();
            }
        },

        async resolveHumanResearch(contactId) {
            const result = await this.apiFetch('contacts/' + contactId, {
                method: 'PATCH',
                body: JSON.stringify({ enrichment_status: 'reviewed', enrichment_notes: null }),
            });
            if (result) {
                this.reports.needsHumanResearch = this.reports.needsHumanResearch.filter(c => c.id !== contactId);
            }
        },

        async enrichWithUrl(contactId, linkedinUrl) {
            if (!linkedinUrl || !linkedinUrl.includes('linkedin.com/in/')) {
                alert('Please enter a valid LinkedIn profile URL (e.g. https://www.linkedin.com/in/name)');
                return;
            }
            const result = await this.apiFetch('contacts/' + contactId, {
                method: 'PATCH',
                body: JSON.stringify({ linkedin_url: linkedinUrl, enrichment_status: null, enrichment_notes: null }),
            });
            if (result) {
                this.reports.needsHumanResearch = this.reports.needsHumanResearch.filter(c => c.id !== contactId);
            }
        },

        async reactivateContact(id) {
            const result = await this.apiFetch('contacts/' + id, {
                method: 'PATCH',
                body: JSON.stringify({ is_active: true, job_change_detected_at: null, linkedin_company_raw: null }),
            });
            if (result && this.detail.show) {
                // Reload company detail to refresh active/inactive lists
                this.openCompanyDetail(this.detail.id);
            }
        },

        async markContactLeft(id) {
            if (!confirm('Mark this person as no longer at this company?')) return;
            const now = new Date().toISOString();
            const result = await this.apiFetch('contacts/' + id, {
                method: 'PATCH',
                body: JSON.stringify({ is_active: false, job_change_detected_at: now }),
            });
            if (result) {
                if (this.detail.data?.contact) {
                    this.detail.data.contact.is_active = false;
                    this.detail.data.contact.job_change_detected_at = now;
                }
            }
        },


        async approveCompanyLinkedIn(companyId) {
            const result = await this.apiFetch('companies/' + companyId, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ is_approved: true }),
            });
            if (result) {
                this.reports.linkedinReview = this.reports.linkedinReview.filter(c => c.id !== companyId);
            }
        },

        async verifyLogoManually(companyId, event) {
            const btn = event?.currentTarget;
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<svg class="animate-spin w-3 h-3" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg> Saving...';
            }
            const result = await this.apiFetch('companies/' + companyId, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ logo_verified: true }),
            });
            if (result && btn) {
                btn.innerHTML = '<svg class="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" /></svg> Verified';
                btn.classList.replace('bg-green-600', 'bg-green-700');
                setTimeout(() => {
                    this.reports.logoReview = this.reports.logoReview.filter(c => c.id !== companyId);
                }, 1000);
            }
        },

        async retryLogoVerification(companyId, event) {
            const btn = event?.currentTarget;
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<svg class="animate-spin w-3 h-3" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg> Resetting...';
            }
            const result = await this.apiFetch('companies/' + companyId, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    logo_verified: null,
                    logo_verified_at: null,
                    logo_hash_website: null,
                    logo_hash_linkedin: null,
                    logo_hash_distance: null,
                }),
            });
            if (result && btn) {
                btn.innerHTML = '<svg class="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" /></svg> Queued';
                btn.classList.replace('bg-primary-600', 'bg-green-600');
                btn.classList.replace('hover:bg-primary-700', 'hover:bg-green-700');
                setTimeout(() => {
                    this.reports.logoReview = this.reports.logoReview.filter(c => c.id !== companyId);
                }, 1000);
            }
        },

        async dismissNoLinkedIn(companyId, event) {
            const btn = event?.currentTarget;
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<svg class="animate-spin w-3 h-3" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg>';
            }
            const result = await this.apiFetch('companies/' + companyId, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ linkedin_name: '[no-linkedin]' }),
            });
            if (result && btn) {
                btn.innerHTML = '<svg class="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" /></svg> Dismissed';
                btn.classList.replace('bg-gray-600', 'bg-green-600');
                setTimeout(() => {
                    this.reports.missingLinkedIn = this.reports.missingLinkedIn.filter(c => c.id !== companyId);
                }, 800);
            }
        },

        // ==================== CONTACT EMAILS ====================
        async loadContactEmails(id, reset = false) {
            if (reset) {
                this.emailsPage = 1;
                this.detail.emails = [];
            }
            this.detail.emailsLoading = true;
            const data = await this.apiFetch('contacts/' + id + '/emails?page=' + this.emailsPage + '&page_size=20');
            if (data) {
                if (reset) {
                    this.detail.emails = data.items || [];
                } else {
                    this.detail.emails = [...(this.detail.emails || []), ...(data.items || [])];
                }
                this.detail.emailsTotal = data.total || 0;
            }
            this.detail.emailsLoading = false;
        },

        async loadMoreEmails() {
            this.emailsPage++;
            await this.loadContactEmails(this.detail.id, false);
        },

        // ==================== INLINE EDITING ====================
        startEditing(field, value) {
            this.editing = { field, value };
            this.$nextTick(() => {
                const input = this.$el?.querySelector('input, select');
                if (input) input.focus();
            });
        },

        cancelEditing() {
            this.editing = { field: null, value: '' };
        },

        async saveField(field) {
            const value = this.editing.value;
            this.editing = { field: null, value: '' };

            if (this.detail.type === 'contact' && this.detail.id) {
                const body = {};
                body[field] = value || null;
                const result = await this.apiFetch('contacts/' + this.detail.id, {
                    method: 'PATCH',
                    body: JSON.stringify(body),
                });
                if (result && this.detail.data) {
                    this.detail.data.contact[field] = value;
                    // Update in list too
                    this.updateContactInList(this.detail.id, body);
                }
            }
        },

        // Company typeahead state
        companySearch: { query: '', results: [], loading: false, selected: null },

        async searchCompanies(query) {
            if (!query || query.length < 2) { this.companySearch.results = []; return; }
            this.companySearch.loading = true;
            const data = await this.apiFetch('companies?search=' + encodeURIComponent(query) + '&page_size=8&contact_filter=all');
            this.companySearch.results = (data?.items || []).slice(0, 8);
            this.companySearch.loading = false;
        },

        async assignCompany(companyId, companyName) {
            this.companySearch = { query: '', results: [], loading: false, selected: null };
            this.editing = { field: null, value: '' };
            const result = await this.apiFetch('contacts/' + this.detail.id, {
                method: 'PATCH',
                body: JSON.stringify({ company_id: companyId }),
            });
            if (result) {
                // Reload contact detail to get fresh company data
                this.openContactDetail(this.detail.id);
            }
        },

        async saveCompanyField(field) {
            const value = this.editing.value;
            this.editing = { field: null, value: '' };
            // Works for both contact detail (company nested) and company detail views
            const companyId = this.detail.type === 'company' ? this.detail.id : this.detail.data?.contact?.company?.id;
            if (!companyId) return;
            const body = {};
            body[field] = value || null;
            const result = await this.apiFetch('companies/' + companyId, {
                method: 'PATCH',
                body: JSON.stringify(body),
            });
            if (result && this.detail.data?.company) {
                this.detail.data.company[field] = value || null;
            }
        },

        async saveCompanyFieldInline(companyId, field, value) {
            const body = {};
            body[field] = value || null;
            const result = await this.apiFetch('companies/' + companyId, {
                method: 'PATCH',
                body: JSON.stringify(body),
            });
            if (result) {
                const item = this.companies.items.find(c => c.id === companyId);
                if (item) item[field] = value || null;
            }
            this._editingCompanyId = null;
        },

        // ==================== APPROVAL TOGGLE ====================
        async toggleApproval(type, id) {
            if (type === 'contact') {
                const contact = this.detail.data?.contact;
                if (!contact) return;
                const newVal = !contact.is_approved;
                const result = await this.apiFetch('contacts/' + id, {
                    method: 'PATCH',
                    body: JSON.stringify({ is_approved: newVal }),
                });
                if (result) {
                    contact.is_approved = newVal;
                    this.updateContactInList(id, { is_approved: newVal });
                }
            } else if (type === 'company') {
                const company = this.detail.data?.company;
                if (!company) return;
                const newVal = !company.is_approved;
                const result = await this.apiFetch('companies/' + id, {
                    method: 'PATCH',
                    body: JSON.stringify({ is_approved: newVal }),
                });
                if (result) {
                    company.is_approved = newVal;
                    const item = this.companies.items.find(c => c.id === id);
                    if (item) item.is_approved = newVal;
                }
            }
        },

        isContactEnriched(contact) {
            return contact.enrichment_status === 'enriched' && contact.title && contact.linkedin_url;
        },

        isCompanyEnriched(company) {
            return !!company.company_type;
        },

        // ==================== VIP TOGGLE ====================
        async toggleVip(contact) {
            const newVal = !contact.is_vip;
            contact.is_vip = newVal;
            await this.apiFetch('contacts/' + contact.id, {
                method: 'PATCH',
                body: JSON.stringify({ is_vip: newVal }),
            });
        },

        async toggleVipDetail() {
            if (!this.detail.data?.contact) return;
            const newVal = !this.detail.data.contact.is_vip;
            this.detail.data.contact.is_vip = newVal;
            await this.apiFetch('contacts/' + this.detail.id, {
                method: 'PATCH',
                body: JSON.stringify({ is_vip: newVal }),
            });
            this.updateContactInList(this.detail.id, { is_vip: newVal });
        },

        // ==================== TAGS ====================
        async addTag() {
            const tag = this.newTag.trim();
            if (!tag || !this.detail.data?.contact) return;
            const tags = [...(this.detail.data.contact.tags || []), tag];
            this.detail.data.contact.tags = tags;
            this.newTag = '';
            await this.apiFetch('contacts/' + this.detail.id, {
                method: 'PATCH',
                body: JSON.stringify({ tags }),
            });
        },

        async removeTag(idx) {
            if (!this.detail.data?.contact) return;
            const tags = [...(this.detail.data.contact.tags || [])];
            tags.splice(idx, 1);
            this.detail.data.contact.tags = tags;
            await this.apiFetch('contacts/' + this.detail.id, {
                method: 'PATCH',
                body: JSON.stringify({ tags }),
            });
        },

        // ==================== NOTES ====================
        async saveNotes(type) {
            if (type === 'contact' && this.detail.data?.contact) {
                await this.apiFetch('contacts/' + this.detail.id, {
                    method: 'PATCH',
                    body: JSON.stringify({ notes: this.detail.data.contact.notes || '' }),
                });
            } else if (type === 'company' && this.detail.data?.company) {
                await this.apiFetch('companies/' + this.detail.id, {
                    method: 'PATCH',
                    body: JSON.stringify({ notes: this.detail.data.company.notes || '' }),
                });
            }
        },

        // ==================== SCAN EMAILS ====================
        async scanCompanyEmails(companyId) {
            this.detail.scanning = true;
            this.detail.scanMessage = '';
            const result = await this.apiFetch('companies/' + companyId + '/scan-emails', {
                method: 'POST',
                body: JSON.stringify({}),
            });
            if (result) {
                this.detail.scanMessage = result.message || 'Scan started';
            } else {
                this.detail.scanMessage = 'Failed to start scan';
            }
            this.detail.scanning = false;
            setTimeout(() => { this.detail.scanMessage = ''; }, 5000);
        },

        // ==================== COMPANY MERGE ====================
        async mergeSearchCompanies(q) {
            if (!q || q.length < 2) {
                this.detail.mergeResults = [];
                return;
            }
            const data = await this.apiFetch('search?q=' + encodeURIComponent(q) + '&limit=10');
            if (data && data.companies) {
                // Exclude the current company from results
                this.detail.mergeResults = data.companies.filter(c => c.id !== this.detail.id);
            }
        },

        async executeMerge(targetId, sourceId, sourceName) {
            if (!confirm('Merge "' + sourceName + '" into this company? This will move all people, news, and enrichments. This cannot be undone.')) {
                return;
            }
            this.detail.merging = true;
            const result = await this.apiFetch('companies/' + targetId + '/merge', {
                method: 'POST',
                body: JSON.stringify({ source_id: sourceId }),
            });
            if (result && result.status === 'merged') {
                this.detail.mergeMode = false;
                this.detail.mergeSearch = '';
                this.detail.mergeResults = [];
                // Refresh company detail
                const data = await this.apiFetch('companies/' + targetId);
                if (data) this.detail.data = data;
                // Refresh companies list
                this.loadCompanies();
            }
            this.detail.merging = false;
        },

        // ==================== GLOBAL SEARCH ====================
        async globalSearch(q) {
            if (!q || q.length < 2) {
                this.searchResults = null;
                this.showSearchResults = false;
                return;
            }
            const data = await this.apiFetch('search?q=' + encodeURIComponent(q) + '&limit=10');
            if (data) {
                this.searchResults = data;
                this.showSearchResults = true;
            }
        },

        // ==================== REPORTS ====================
        // ==================== LINKEDIN ====================
        async loadLinkedIn() {
            this.linkedin.loading = true;
            const [posts, jobChanges, titleChanges, summary] = await Promise.all([
                this.apiFetch('reports/new-linkedin-posts'),
                this.apiFetch('reports/job-changes'),
                this.apiFetch('reports/title-changes'),
                this.apiFetch('reports/contact-activity-summary'),
            ]);
            if (posts) this.linkedin.posts = posts.items || [];
            if (posts) this.linkedin.postsTotal = posts.total || 0;
            if (jobChanges) this.linkedin.jobChanges = jobChanges.items || [];
            if (titleChanges) this.linkedin.titleChanges = titleChanges.items || [];
            if (summary) this.linkedin.summary = summary;
            this.linkedin.loading = false;
        },

        async markPostSeen(postId) {
            await this.apiFetch('linkedin-posts/' + postId + '/mark-seen', { method: 'POST' });
            this.linkedin.posts = this.linkedin.posts.filter(p => p.id !== postId);
            this.linkedin.postsTotal = Math.max(0, this.linkedin.postsTotal - 1);
        },

        async markAllPostsSeen() {
            await this.apiFetch('linkedin-posts/mark-all-seen', { method: 'POST' });
            this.linkedin.posts = [];
            this.linkedin.postsTotal = 0;
        },

        async setContactTier(contactId, tier) {
            await this.apiFetch('contacts/' + contactId + '/monitoring-tier', {
                method: 'POST',
                body: JSON.stringify({ tier }),
            });
        },

        truncateText(text, maxLen) {
            if (!text) return '';
            if (text.length <= maxLen) return text;
            return text.substring(0, maxLen) + '...';
        },

        tierBadgeClass(tier) {
            const map = {
                'A': 'bg-green-100 text-green-700',
                'B': 'bg-blue-100 text-blue-700',
                'C': 'bg-gray-100 text-gray-600',
            };
            return map[tier] || 'bg-gray-50 text-gray-400';
        },

        async loadReports() {
            this.reports.loading = true;
            const [noPeople, needsLI, browserEnrich, humanResearch, jobChanges, needsLeadership, linkedinReview, missingLinkedIn, logoReview] = await Promise.all([
                this.apiFetch('reports/companies-without-people'),
                this.apiFetch('reports/needs-linkedin-url'),
                this.apiFetch('reports/needs-browser-enrich'),
                this.apiFetch('reports/needs-human-research'),
                this.apiFetch('reports/job-changes'),
                this.apiFetch('reports/needs-leadership-discovery'),
                this.apiFetch('reports/needs-company-linkedin-review'),
                this.apiFetch('reports/missing-company-linkedin'),
                this.apiFetch('reports/logo-review'),
            ]);
            if (noPeople) this.reports.companiesWithoutPeople = noPeople.items || [];
            if (needsLI) this.reports.needsLinkedIn = needsLI.items || [];
            if (browserEnrich) this.reports.needsBrowserEnrich = browserEnrich.items || [];
            if (humanResearch) this.reports.needsHumanResearch = humanResearch.items || [];
            if (jobChanges) this.reports.jobChanges = jobChanges.items || [];
            if (needsLeadership) this.reports.needsLeadership = needsLeadership.items || [];
            if (linkedinReview) this.reports.linkedinReview = linkedinReview.items || [];
            if (missingLinkedIn) this.reports.missingLinkedIn = missingLinkedIn.items || [];
            if (logoReview) this.reports.logoReview = logoReview.items || [];
            this.reports.loading = false;
        },

        // ==================== OUTREACH ====================
        async loadOutreach() {
            this.outreach.loading = true;
            const [stats, suggestions] = await Promise.all([
                this.apiFetch('outreach/dashboard'),
                this.apiFetch('outreach/suggestions?page=' + this.outreach.page + '&page_size=' + this.outreach.pageSize + '&status=' + this.outreach.filter),
            ]);
            if (stats) this.outreach.stats = stats;
            if (suggestions) {
                this.outreach.items = suggestions.items || [];
                this.outreach.total = suggestions.total || 0;
                this.outreach.totalPages = suggestions.total_pages || 0;
            }
            this.outreach.loading = false;
        },

        async loadSuggestions() {
            const data = await this.apiFetch('outreach/suggestions?page=' + this.outreach.page + '&page_size=' + this.outreach.pageSize + '&status=' + this.outreach.filter);
            if (data) {
                this.outreach.items = data.items || [];
                this.outreach.total = data.total || 0;
                this.outreach.totalPages = data.total_pages || 0;
            }
        },

        async loadNewsFeed() {
            this.outreach.newsLoading = true;
            const params = new URLSearchParams({
                page: this.outreach.newsPage,
                page_size: '50',
                sort_by: 'created_at',
                sort_dir: 'desc',
            });
            if (this.outreach.newsFilter && this.outreach.newsFilter !== 'all') {
                params.set('status', this.outreach.newsFilter);
            }
            const data = await this.apiFetch('outreach/news?' + params.toString());
            if (data) {
                this.outreach.news = data.items || [];
                this.outreach.newsTotal = data.total || 0;
                this.outreach.newsTotalPages = data.total_pages || 0;
            }
            this.outreach.newsLoading = false;
        },

        switchOutreachTab(tab) {
            this.outreach.tab = tab;
            if (tab === 'news' && this.outreach.news.length === 0) {
                this.loadNewsFeed();
            }
        },

        startEditSuggestion(s) {
            this.outreach.editingId = s.id;
            this.outreach.editSubject = s.subject;
            this.outreach.editBody = s.body;
        },

        async saveSuggestionEdit(id) {
            await this.apiFetch('outreach/suggestions/' + id, {
                method: 'PATCH',
                body: JSON.stringify({ subject: this.outreach.editSubject, body: this.outreach.editBody }),
            });
            this.outreach.editingId = null;
            this.loadSuggestions();
        },

        async updateSuggestionStatus(id, status) {
            await this.apiFetch('outreach/suggestions/' + id, {
                method: 'PATCH',
                body: JSON.stringify({ status }),
            });
            this.loadSuggestions();
            // Refresh stats
            const stats = await this.apiFetch('outreach/dashboard');
            if (stats) this.outreach.stats = stats;
        },

        async triggerPipeline() {
            this.outreach.pipelineRunning = true;
            await this.apiFetch('outreach/trigger', { method: 'POST' });
            // Pipeline runs async - just update UI state
            setTimeout(() => { this.outreach.pipelineRunning = false; }, 5000);
        },

        outreachCategoryBadge(cat) {
            const map = {
                'project_win': 'bg-green-100 text-green-800',
                'project_completion': 'bg-blue-100 text-blue-800',
                'executive_hire': 'bg-purple-100 text-purple-800',
                'expansion': 'bg-amber-100 text-amber-800',
                'partnership': 'bg-teal-100 text-teal-800',
                'award': 'bg-yellow-100 text-yellow-800',
                'financial_results': 'bg-indigo-100 text-indigo-800',
            };
            return map[cat] || 'bg-gray-100 text-gray-700';
        },

        outreachCategoryLabel(cat) {
            const map = {
                'project_win': 'Project Win',
                'project_completion': 'Completion',
                'executive_hire': 'New Hire',
                'expansion': 'Expansion',
                'partnership': 'Partnership',
                'award': 'Award',
                'financial_results': 'Financial',
            };
            return map[cat] || cat || 'News';
        },

        // ==================== SORTING ====================
        toggleSort(entity, field) {
            const state = this[entity];
            if (state.sortBy === field) {
                state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
            } else {
                state.sortBy = field;
                state.sortDir = 'desc';
            }
            state.page = 1;
            if (entity === 'contacts') this.loadContacts();
            else this.loadCompanies();
        },

        // ==================== LIST SYNC ====================
        updateContactInList(id, updates) {
            const idx = this.contacts.items.findIndex((c) => c.id === id);
            if (idx >= 0) {
                Object.assign(this.contacts.items[idx], updates);
            }
        },

        // ==================== BADGE HELPERS ====================
        contactTypeBadge(type) {
            const map = {
                'Champion': 'bg-green-100 text-green-800',
                'Decision Maker': 'bg-purple-100 text-purple-800',
                'Influencer': 'bg-blue-100 text-blue-800',
                'End User': 'bg-gray-100 text-gray-700',
                'Executive Sponsor': 'bg-indigo-100 text-indigo-800',
                'Blocker': 'bg-red-100 text-red-800',
            };
            return map[type] || 'bg-gray-100 text-gray-700';
        },

        companyTypeBadge(type) {
            const map = {
                'Customer': 'bg-green-100 text-green-800',
                'Prospect': 'bg-blue-100 text-blue-800',
                'Partner': 'bg-purple-100 text-purple-800',
                'Vendor': 'bg-orange-100 text-orange-800',
                'Trade Group': 'bg-amber-100 text-amber-800',
            };
            return map[type] || 'bg-gray-100 text-gray-700';
        },

        tierBadge(tier) {
            const map = {
                'Enterprise': 'bg-indigo-100 text-indigo-800',
                'Mid-Market': 'bg-blue-100 text-blue-800',
                'SMB': 'bg-gray-100 text-gray-700',
            };
            return map[tier] || 'bg-gray-100 text-gray-700';
        },

        // ==================== FORMAT HELPERS ====================
        formatNumber(n) {
            if (n == null) return '0';
            return Number(n).toLocaleString();
        },

        formatDate(d) {
            if (!d) return '-';
            const date = new Date(d);
            if (isNaN(date.getTime())) return '-';
            return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        },

        formatCurrency(n) {
            if (n == null) return '-';
            const num = Number(n);
            if (num >= 1000000) return '$' + (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
            if (num >= 1000) return '$' + (num / 1000).toFixed(0) + 'K';
            return '$' + num.toFixed(0);
        },

        timeAgo(d) {
            if (!d) return '';
            const now = new Date();
            const date = new Date(d);
            if (isNaN(date.getTime())) return '';
            const seconds = Math.floor((now - date) / 1000);

            if (seconds < 60) return 'just now';
            const minutes = Math.floor(seconds / 60);
            if (minutes < 60) return minutes + (minutes === 1 ? ' min ago' : ' mins ago');
            const hours = Math.floor(minutes / 60);
            if (hours < 24) return hours + (hours === 1 ? ' hour ago' : ' hours ago');
            const days = Math.floor(hours / 24);
            if (days < 30) return days + (days === 1 ? ' day ago' : ' days ago');
            const months = Math.floor(days / 30);
            if (months < 12) return months + (months === 1 ? ' month ago' : ' months ago');
            const years = Math.floor(months / 12);
            return years + (years === 1 ? ' year ago' : ' years ago');
        },

        isRenewalSoon(dateStr) {
            if (!dateStr) return false;
            const date = new Date(dateStr);
            if (isNaN(date.getTime())) return false;
            const now = new Date();
            const diffDays = (date - now) / (1000 * 60 * 60 * 24);
            return diffDays >= 0 && diffDays <= 30;
        },
    };
}
