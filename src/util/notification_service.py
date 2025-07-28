import smtplib
import json
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

class NotificationService:
    """
    Service for sending notifications about ETL job status and errors.
    Supports email, Slack, and webhook notifications.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.logger = logging.getLogger(__name__)
        self.config = config or {}
        
        # Email configuration
        self.smtp_server = self.config.get('smtp_server', 'localhost')
        self.smtp_port = self.config.get('smtp_port', 587)
        self.smtp_username = self.config.get('smtp_username')
        self.smtp_password = self.config.get('smtp_password')
        self.from_email = self.config.get('from_email', 'etl@company.com')
        
        # Slack configuration
        self.slack_webhook_url = self.config.get('slack_webhook_url')
        self.slack_channel = self.config.get('slack_channel', '#etl-alerts')
        
        # Webhook configuration
        self.webhook_urls = self.config.get('webhook_urls', [])
    
    def send_email(self, to_emails: List[str], subject: str, 
                  message: str, html_message: Optional[str] = None) -> bool:
        """
        Send email notification.
        
        Args:
            to_emails: List of recipient email addresses
            subject: Email subject
            message: Plain text message
            html_message: HTML message (optional)
            
        Returns:
            bool: True if successful
        """
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to_emails)
            
            # Add plain text part
            text_part = MIMEText(message, 'plain')
            msg.attach(text_part)
            
            # Add HTML part if provided
            if html_message:
                html_part = MIMEText(html_message, 'html')
                msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.smtp_username and self.smtp_password:
                    server.starttls()
                    server.login(self.smtp_username, self.smtp_password)
                
                server.send_message(msg)
            
            self.logger.info(f"Email sent successfully to {', '.join(to_emails)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return False
    
    def send_slack_notification(self, message: str, channel: Optional[str] = None) -> bool:
        """
        Send Slack notification.
        
        Args:
            message: Message to send
            channel: Slack channel (optional, uses default if not provided)
            
        Returns:
            bool: True if successful
        """
        if not self.slack_webhook_url:
            self.logger.warning("Slack webhook URL not configured")
            return False
        
        try:
            payload = {
                'text': message,
                'channel': channel or self.slack_channel,
                'username': 'ETL Framework'
            }
            
            response = requests.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                self.logger.info("Slack notification sent successfully")
                return True
            else:
                self.logger.error(f"Slack notification failed: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {str(e)}")
            return False
    
    def send_webhook_notification(self, data: Dict[str, Any]) -> bool:
        """
        Send webhook notification.
        
        Args:
            data: Data to send in webhook
            
        Returns:
            bool: True if all webhooks successful
        """
        if not self.webhook_urls:
            self.logger.warning("No webhook URLs configured")
            return True
        
        success = True
        
        for webhook_url in self.webhook_urls:
            try:
                response = requests.post(
                    webhook_url,
                    json=data,
                    timeout=10
                )
                
                if response.status_code in [200, 201, 202]:
                    self.logger.info(f"Webhook notification sent to {webhook_url}")
                else:
                    self.logger.error(f"Webhook failed for {webhook_url}: {response.status_code}")
                    success = False
                    
            except Exception as e:
                self.logger.error(f"Failed to send webhook to {webhook_url}: {str(e)}")
                success = False
        
        return success
    
    def send_job_success_notification(self, job_name: str, 
                                    recipients: List[str],
                                    job_details: Optional[Dict] = None) -> bool:
        """
        Send job success notification.
        
        Args:
            job_name: Name of the job
            recipients: List of email recipients
            job_details: Job execution details
            
        Returns:
            bool: True if successful
        """
        subject = f"ETL Job Success: {job_name}"
        
        # Build message
        details = job_details or {}
        message = f"""
ETL Job '{job_name}' completed successfully.

Job Details:
- Start Time: {details.get('start_time', 'Unknown')}
- End Time: {details.get('end_time', 'Unknown')}
- Duration: {details.get('duration', 'Unknown')}
- Rows Processed: {details.get('rows_processed', 'Unknown')}
- Tables Processed: {details.get('tables_processed', 'Unknown')}

Status: SUCCESS
        """.strip()
        
        # Send email
        email_success = self.send_email(recipients, subject, message)
        
        # Send Slack notification
        slack_message = f"✅ ETL Job Success: {job_name} completed successfully"
        slack_success = self.send_slack_notification(slack_message)
        
        # Send webhook
        webhook_data = {
            'event_type': 'job_success',
            'job_name': job_name,
            'timestamp': datetime.now().isoformat(),
            'details': details
        }
        webhook_success = self.send_webhook_notification(webhook_data)
        
        return email_success and slack_success and webhook_success
    
    def send_job_failure_notification(self, job_name: str,
                                    recipients: List[str],
                                    error_message: str,
                                    job_details: Optional[Dict] = None) -> bool:
        """
        Send job failure notification.
        
        Args:
            job_name: Name of the job
            recipients: List of email recipients
            error_message: Error message
            job_details: Job execution details
            
        Returns:
            bool: True if successful
        """
        subject = f"ETL Job FAILED: {job_name}"
        
        # Build message
        details = job_details or {}
        message = f"""
ETL Job '{job_name}' FAILED.

Error Message:
{error_message}

Job Details:
- Start Time: {details.get('start_time', 'Unknown')}
- Failure Time: {details.get('failure_time', 'Unknown')}
- Duration: {details.get('duration', 'Unknown')}
- Rows Processed: {details.get('rows_processed', 'Unknown')}

Status: FAILED

Please check the logs for more details.
        """.strip()
        
        # Send email
        email_success = self.send_email(recipients, subject, message)
        
        # Send Slack notification
        slack_message = f"❌ ETL Job FAILED: {job_name} - {error_message}"
        slack_success = self.send_slack_notification(slack_message)
        
        # Send webhook
        webhook_data = {
            'event_type': 'job_failure',
            'job_name': job_name,
            'timestamp': datetime.now().isoformat(),
            'error_message': error_message,
            'details': details
        }
        webhook_success = self.send_webhook_notification(webhook_data)
        
        return email_success and slack_success and webhook_success
    
    def send_error_notification(self, subject: str, message: str, 
                              error_details: Optional[Dict] = None) -> bool:
        """
        Send generic error notification.
        
        Args:
            subject: Error subject
            message: Error message
            error_details: Additional error details
            
        Returns:
            bool: True if successful
        """
        # Get admin emails from config
        admin_emails = self.config.get('admin_emails', ['admin@company.com'])
        
        # Build detailed message
        detailed_message = message
        if error_details:
            detailed_message += f"\n\nError Details:\n{json.dumps(error_details, indent=2)}"
        
        # Send email
        email_success = self.send_email(admin_emails, subject, detailed_message)
        
        # Send Slack notification
        slack_message = f"🚨 {subject}: {message}"
        slack_success = self.send_slack_notification(slack_message)
        
        # Send webhook
        webhook_data = {
            'event_type': 'error',
            'subject': subject,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'error_details': error_details
        }
        webhook_success = self.send_webhook_notification(webhook_data)
        
        return email_success and slack_success and webhook_success