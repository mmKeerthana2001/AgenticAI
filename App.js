import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const App = () => {
  const [agentStatus, setAgentStatus] = useState('stopped');
  const [sessionId, setSessionId] = useState(null);
  const [tickets, setTickets] = useState([]);
  const [adminRequest, setAdminRequest] = useState('');
  const [isSending, setIsSending] = useState(false);
  const [modals, setModals] = useState([]);
  const [wsError, setWsError] = useState(null);
  const [expandedTickets, setExpandedTickets] = useState({});

  // WebSocket connection
  useEffect(() => {
    let ws;
    let reconnectAttempts = 0;
    const maxAttempts = 10;

    const connectWebSocket = () => {
      ws = new WebSocket('ws://localhost:8000/ws');
      ws.onopen = () => {
        console.log('WebSocket connected');
        setWsError(null);
        reconnectAttempts = 0;
      };
      ws.onmessage = (event) => {
        const message = JSON.parse(event.data);
        console.log('WebSocket message:', message);

        setModals((prev) => {
          const existingModalIndex = prev.findIndex(m => m.email_id === message.email_id);
          let updatedModals = [...prev];

          if (message.type === 'session') {
            setAgentStatus(message.status);
            setSessionId(message.session_id);
            return prev;
          } else if (message.type === 'email_detected') {
            const isValidDomain = message.is_valid_domain !== false;
            if (existingModalIndex === -1) {
              return [
                ...prev,
                {
                  email_id: message.email_id,
                  steps: [{
                    status: 'New email arrived',
                    details: `Subject: ${message.subject}, From: ${message.sender}${!isValidDomain ? ' - UNAUTHORIZED DOMAIN' : ''}`
                  }],
                  show: true,
                  isSpam: !isValidDomain
                }
              ];
            }
          } else if (message.type === 'spam_alert') {
            if (existingModalIndex !== -1) {
              updatedModals[existingModalIndex] = {
                ...updatedModals[existingModalIndex],
                steps: [
                  ...updatedModals[existingModalIndex].steps,
                  {
                    status: 'SECURITY ALERT',
                    details: `<span class="text-red">⚠️ ${message.message}</span>`
                  }
                ],
                isSpam: true
              };
              return updatedModals;
            }
          } else if (message.type === 'intent_analyzed') {
            if (existingModalIndex !== -1) {
              const hasIntent = updatedModals[existingModalIndex].steps.some(
                step => step.status === 'Analyzing intent' && step.details === `Intent: ${message.intent}`
              );
              if (!hasIntent) {
                updatedModals[existingModalIndex] = {
                  ...updatedModals[existingModalIndex],
                  steps: [
                    ...updatedModals[existingModalIndex].steps,
                    { status: 'Analyzing intent', details: `Intent: ${message.intent}` }
                  ]
                };
              }
              return updatedModals;
            }
          } else if (message.type === 'ticket_created') {
            if (existingModalIndex !== -1) {
              const hasTicket = updatedModals[existingModalIndex].steps.some(
                step => step.status === 'Created ADO ticket' && step.details.includes(`ID: ${message.ticket_id}`)
              );
              if (!hasTicket) {
                updatedModals[existingModalIndex] = {
                  ...updatedModals[existingModalIndex],
                  steps: [
                    ...updatedModals[existingModalIndex].steps,
                    {
                      status: 'Created ADO ticket',
                      details: `ID: ${message.ticket_id}, <a href="${message.ado_url}" target="_blank" class="text-blue">View Ticket</a>`
                    }
                  ]
                };
              }
              return updatedModals;
            }
          } else if (message.type === 'action_performed') {
            const actionLabels = {
              github_create_repo: 'Created GitHub repository',
              github_commit_file: 'Committed file to repository',
              github_delete_repo: 'Deleted GitHub repository',
              github_access_request: 'Granted repository access',
              github_revoke_access: 'Revoked repository access',
              aws_s3_create_bucket: 'Created S3 bucket',
              aws_s3_delete_bucket: 'Deleted S3 bucket',
              aws_ec2_launch_instance: 'Launched EC2 instance',
              aws_ec2_terminate_instance: 'Terminated EC2 instance',
              aws_iam_add_user: 'Added IAM user',
              aws_iam_remove_user: 'Removed IAM user',
              aws_iam_add_user_permission: 'Added IAM user permission',
              aws_iam_remove_user_permission: 'Removed IAM user permission'
            };
            const actionStatus = actionLabels[message.intent] || 'Performed action';
            const actionDetails = message.success ? `Completed: ${message.message}` : `Failed: ${message.message}`;
            
            if (existingModalIndex !== -1) {
              const hasAction = updatedModals[existingModalIndex].steps.some(
                step => step.status === actionStatus && step.details === actionDetails
              );
              if (!hasAction) {
                updatedModals[existingModalIndex] = {
                  ...updatedModals[existingModalIndex],
                  steps: [
                    ...updatedModals[existingModalIndex].steps,
                    { status: actionStatus, details: actionDetails }
                  ]
                };
              }
              return updatedModals;
            }
          } else if (message.type === 'ticket_updated') {
            if (existingModalIndex !== -1) {
              updatedModals[existingModalIndex] = {
                ...updatedModals[existingModalIndex],
                steps: [
                  ...updatedModals[existingModalIndex].steps,
                  {
                    status: 'Updated work item',
                    details: `Status: ${message.status}, Comment: ${message.comment}`
                  }
                ]
              };
            }
            setTickets((prevTickets) =>
              prevTickets.map((ticket) =>
                ticket.ado_ticket_id === message.ticket_id?.toString()
                  ? {
                      ...ticket,
                      updates: [
                        ...ticket.updates,
                        {
                          status: message.status,
                          comment: message.comment || 'No comment provided',
                          revision_id: message.revision_id || `update-${Date.now()}`,
                          email_sent: false,
                          email_message_id: null,
                          email_timestamp: new Date().toISOString()
                        }
                      ]
                    }
                  : ticket
              )
            );
            return updatedModals;
          } else if (message.type === 'email_reply') {
            if (existingModalIndex !== -1) {
              updatedModals[existingModalIndex] = {
                ...updatedModals[existingModalIndex],
                steps: [
                  ...updatedModals[existingModalIndex].steps,
                  { status: 'Sent reply to user', details: `Thread ID: ${message.thread_id}` }
                ]
              };
            }
            setTickets((prevTickets) =>
              prevTickets.map((ticket) =>
                ticket.email_id === message.email_id
                  ? {
                      ...ticket,
                      updates: ticket.updates.map((update, index) =>
                        index === ticket.updates.length - 1
                          ? { ...update, email_sent: true, email_message_id: message.message_id || `reply-${Date.now()}` }
                          : update
                      )
                    }
                  : ticket
              )
            );
            return updatedModals;
          }
          return prev;
        });
      };
      ws.onerror = (error) => {
        console.log('WebSocket error:', error);
        setWsError('WebSocket connection failed');
      };
      ws.onclose = () => {
        console.log('WebSocket disconnected');
        if (reconnectAttempts < maxAttempts) {
          reconnectAttempts++;
          setTimeout(connectWebSocket, 3000);
        } else {
          setWsError('WebSocket disconnected after max retries');
        }
      };
    };

    connectWebSocket();
    return () => ws && ws.close();
  }, []);

  // Fetch tickets
  useEffect(() => {
    const fetchData = async () => {
      try {
        const ticketsRes = await axios.get('http://localhost:8000/tickets');
        console.log('Fetched tickets:', ticketsRes.data.tickets);
        setTickets(ticketsRes.data.tickets || []);
      } catch (error) {
        console.error('Error fetching tickets:', error);
      }
    };
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  // Handle agent toggle
  const handleAgentToggle = async () => {
    const newStatus = agentStatus === 'stopped' ? 'started' : 'stopped';
    const endpoint = agentStatus === 'stopped' ? '/run-agent' : '/stop-agent';
    try {
      console.log(`Calling endpoint: ${endpoint}`);
      setAgentStatus(newStatus); // Set state immediately for UI feedback
      const res = await axios.get(`http://localhost:8000${endpoint}`);
      console.log('API response:', res.data);
      // Log success but don't revert state
      console.log(`Agent ${newStatus === 'started' ? 'started' : 'stopped'} successfully`);
    } catch (error) {
      console.error(`Error ${newStatus === 'started' ? 'starting' : 'stopping'} agent:`, error);
      setWsError(`Failed to ${newStatus === 'started' ? 'start' : 'stop'} agent`);
      // Don't revert state on error to avoid flicker; handle manually if needed
    }
  };
  // Handle sending admin request
  const handleSendRequest = async () => {
    if (!adminRequest.trim()) {
      alert('Please enter a request.');
      return;
    }
    const ticketIdMatch = adminRequest.match(/\b(\d+)\b/);
    const ticketId = ticketIdMatch ? parseInt(ticketIdMatch[0]) : null;
    if (!ticketId) {
      setModals((prev) => [
        ...prev,
        {
          email_id: `admin-request-${Date.now()}`,
          steps: [{ status: 'Error', details: 'No valid ticket ID found in the request.' }],
          show: true,
          isSpam: false
        }
      ]);
      return;
    }
    setIsSending(true);
    try {
      const res = await axios.post('http://localhost:8000/send-request', {
        ticket_id: ticketId,
        request: adminRequest
      });
      setModals((prev) => [
        ...prev,
        {
          email_id: `admin-request-${Date.now()}`,
          steps: [
            { status: 'Admin Request', details: adminRequest },
            {
              status: res.data.summary_intent === 'error' ? 'Error' : 'Response',
              details: res.data.response.replace(/\n/g, '<br>')
            }
          ],
          show: true,
          isSpam: false
        }
      ]);
      setAdminRequest('');
    } catch (error) {
      console.error('Error sending admin request:', error);
      setModals((prev) => [
        ...prev,
        {
          email_id: `admin-request-${Date.now()}`,
          steps: [
            { status: 'Admin Request', details: adminRequest },
            { status: 'Error', details: `Failed to process request: ${error.response?.data?.detail || error.message}` }
          ],
          show: true,
          isSpam: false
        }
      ]);
    } finally {
      setIsSending(false);
    }
  };

  // Close modal
  const closeModal = (email_id) => {
    setModals((prev) => prev.filter((modal) => modal.email_id !== email_id));
  };

  // Toggle ticket details
  const toggleTicketDetails = (ticketId) => {
    setExpandedTickets((prev) => ({
      ...prev,
      [ticketId]: !prev[ticketId]
    }));
  };

  return (
    <div className="app">
      {/* Navbar */}
      <header className="navbar">
        <div className="navbar-container">
          <h1 className="navbar-title">IT Support Dashboard</h1>
          <nav>
            <button className="nav-button">Profile</button>
          </nav>
        </div>
      </header>

      {/* Main Content */}
      <main className="main-container">
        <h2 className="dashboard-title">Admin Space</h2>
        
        {/* Send Request */}
        <section className="send-request">
          <div className="request-card">
            <div className="request-wrapper">
              <textarea
                className="request-input"
                placeholder="Enter your request (e.g., Summarize ticket ID 208)"
                value={adminRequest}
                onChange={(e) => setAdminRequest(e.target.value)}
                rows="3"
              />
              <div className="request-actions">
                <button className="voice-button" title="Voice Input (Coming Soon)">
                  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <rect x="9" y="2" width="6" height="12" rx="3" ry="3"></rect>
                    <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
                    <line x1="12" y1="19" x2="12" y2="22"></line>
                    <line x1="9" y1="22" x2="15" y2="22"></line>
                  </svg>
                </button>
                <button
                  className={`send-button ${isSending ? 'button-disabled' : ''}`}
                  onClick={handleSendRequest}
                  disabled={isSending}
                >
                  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <line x1="22" y1="2" x2="11" y2="13"></line>
                    <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
                  </svg>
                </button>
              </div>
            </div>
          </div>
        </section>

        {/* Tickets Table */}
        <section className="ticket-section">
          <div className="card">
            <h3 className="card-title">Tickets</h3>
            {tickets.length === 0 ? (
              <p className="no-data">No tickets available.</p>
            ) : (
              <div className="ticket-table-wrapper">
                <table className="ticket-table">
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Title</th>
                      <th>Sender</th>
                      <th>Status</th>
                      <th>Pending Actions</th>
                      <th>Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tickets.map((ticket) => (
                      <React.Fragment key={ticket.ado_ticket_id}>
                        <tr className="ticket-row">
                          <td>{ticket.ado_ticket_id}</td>
                          <td>{ticket.ticket_title}</td>
                          <td>{ticket.sender}</td>
                          <td>{ticket.updates?.length > 0 ? ticket.updates[ticket.updates.length - 1].status : 'New'}</td>
                          <td>{ticket.pending_actions ? 'Yes' : 'No'}</td>
                          <td>
                            <button
                              className="details-button"
                              onClick={() => toggleTicketDetails(ticket.ado_ticket_id)}
                            >
                              {expandedTickets[ticket.ado_ticket_id] ? 'Hide' : 'Show'}
                            </button>
                          </td>
                        </tr>
                        {expandedTickets[ticket.ado_ticket_id] && (
                          <tr className="details-row">
                            <td colSpan="6">
                              <div className="ticket-details">
                                <h4 className="section-title">Description</h4>
                                <p className="ticket-info">{ticket.ticket_description}</p>
                                <h4 className="section-title">Details</h4>
                                <p className="ticket-info">Type: {ticket.type_of_request}</p>
                                {ticket.details?.github && (
                                  <div>
                                    <h5 className="sub-section-title">GitHub Actions</h5>
                                    <ul className="action-list">
                                      {ticket.details.github.map((action, index) => (
                                        <li key={index}>
                                          {action.action.replace('github_', '').replace('_', ' ').toUpperCase()}: {action.completed ? 'Completed' : 'Pending'}
                                        </li>
                                      ))}
                                    </ul>
                                  </div>
                                )}
                                {ticket.details?.aws && (
                                  <div>
                                    <h5 className="sub-section-title">AWS Actions</h5>
                                    <ul className="action-list">
                                      {ticket.details.aws.map((action, index) => (
                                        <li key={index}>
                                          {action.action.replace('aws_', '').replace('_', ' ').toUpperCase()}: {action.completed ? 'Completed' : 'Pending'}
                                        </li>
                                      ))}
                                    </ul>
                                  </div>
                                )}
                                {ticket.details?.attachments && (
                                  <div>
                                    <h5 className="sub-section-title">Attachments</h5>
                                    <ul className="action-list">
                                      {ticket.details.attachments.map((attachment, index) => (
                                        <li key={index}>{attachment.filename} ({attachment.mimeType})</li>
                                      ))}
                                    </ul>
                                  </div>
                                )}
                                <h4 className="section-title">Updates</h4>
                                <div className="update-list">
                                  {ticket.updates.map((update, index) => (
                                    <div key={index} className="update-item">
                                      <p>Status: {update.status}</p>
                                      <p>Comment: {update.comment}</p>
                                      <p>Timestamp: {new Date(update.email_timestamp).toLocaleString()}</p>
                                      <p>Email Sent: {update.email_sent ? 'Yes' : 'No'}</p>
                                    </div>
                                  ))}
                                </div>
                                <h4 className="section-title">Email Chain</h4>
                                <div className="email-list">
                                  {ticket.email_chain.map((email, index) => (
                                    <div key={index} className="email-item">
                                      <p><strong>From:</strong> {email.from}</p>
                                      <p><strong>Subject:</strong> {email.subject}</p>
                                      <p><strong>Timestamp:</strong> {new Date(parseInt(email.timestamp)).toLocaleString()}</p>
                                      <p><strong>Body:</strong></p>
                                      <p className="email-body">{email.body}</p>
                                      {email.attachments?.length > 0 && (
                                        <div>
                                          <p><strong>Attachments:</strong></p>
                                          <ul className="action-list">
                                            {email.attachments.map((attachment, idx) => (
                                              <li key={idx}>{attachment.filename} ({attachment.mimeType})</li>
                                            ))}
                                          </ul>
                                        </div>
                                      )}
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </section>
      </main>

      {/* Agent Control (Floating) */}
      <button
        className={`agent-button ${agentStatus === 'stopped' ? 'start-button' : 'stop-button'}`}
        onClick={handleAgentToggle}
      >
        {agentStatus === 'stopped' ? 'Start Agent' : 'Stop Agent'}
      </button>

      {/* Workflow Modals */}
      {modals.map((modal) => (
        <div
          key={modal.email_id}
          className={`modal ${modal.show ? 'modal-visible' : ''} ${modal.isSpam ? 'modal-spam' : ''}`}
        >
          <div className="modal-content">
            <button
              className="modal-close"
              onClick={() => closeModal(modal.email_id)}
              title="Close"
            >
              ×
            </button>
            <h3 className="modal-title">
              {modal.isSpam
                ? '⚠️ Unauthorized Email Alert'
                : modal.steps.some(step => step.status === 'Admin Request')
                  ? 'Admin Request Response'
                  : 'Email Processing Workflow'} (ID: {modal.email_id})
            </h3>
            <div className="modal-steps">
              {modal.steps.map((step, index) => (
                <div
                  key={`${step.status}-${index}`}
                  className={`step-item ${step.status === 'SECURITY ALERT' ? 'step-alert' : ''}`}
                >
                  <p className="step-status">{step.status}</p>
                  <p className="step-details" dangerouslySetInnerHTML={{ __html: step.details }} />
                </div>
              ))}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
};

export default App;