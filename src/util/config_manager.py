import yaml
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union
import logging
from datetime import datetime


class ConfigManager:
    """
    Configuration manager for the ETL framework.
    Handles loading, validation, and management of configuration files.
    """
    
    def __init__(self, config_dir: Union[str, Path] = "config"):
        self.config_dir = Path(config_dir)
        self.logger = logging.getLogger(__name__)
        self._configs = {}
        self._load_all_configs()
    
    def _load_all_configs(self):
        """Load all configuration files from the config directory."""
        if not self.config_dir.exists():
            self.logger.warning(f"Config directory {self.config_dir} does not exist")
            return
        
        config_files = {
            'database': 'database_configs.yml',
            'etl_jobs': 'etl_jobs.yml',
            'logging': 'logging_config.yml',
            'schedule': 'schedule_config.yml',
            'transformation': 'transformation_rules.yml'
        }
        
        for config_type, filename in config_files.items():
            config_path = self.config_dir / filename
            if config_path.exists():
                try:
                    self._configs[config_type] = self._load_config_file(config_path)
                    self.logger.info(f"Loaded {config_type} configuration from {filename}")
                except Exception as e:
                    self.logger.error(f"Failed to load {config_type} config: {str(e)}")
            else:
                self.logger.warning(f"Config file {filename} not found")
    
    def _load_config_file(self, file_path: Path) -> Dict[str, Any]:
        """Load a single configuration file."""
        with open(file_path, 'r') as f:
            if file_path.suffix in ['.yml', '.yaml']:
                # Handle multiple YAML documents
                configs = list(yaml.safe_load_all(f))
                if len(configs) == 1:
                    return configs[0]
                else:
                    # Merge multiple documents
                    merged_config = {}
                    for config in configs:
                        if config:
                            merged_config.update(config)
                    return merged_config
            elif file_path.suffix == '.json':
                return json.load(f)
            else:
                raise ValueError(f"Unsupported config file format: {file_path.suffix}")
    
    def get_database_config(self, database_name: str) -> Dict[str, Any]:
        """
        Get database configuration by name.
        
        Args:
            database_name: Name of the database configuration
            
        Returns:
            Dict: Database configuration
        """
        db_configs = self._configs.get('database', {})
        
        # Try different possible keys
        for key in ['oracle_databases', 'databases']:
            if key in db_configs and database_name in db_configs[key]:
                config = db_configs[key][database_name].copy()
                
                # Apply default configuration if available
                if 'default_oracle_config' in db_configs:
                    default_config = db_configs['default_oracle_config'].copy()
                    default_config.update(config)
                    config = default_config
                
                # Resolve environment variables
                config = self._resolve_environment_variables(config)
                
                return config
        
        raise ValueError(f"Database configuration '{database_name}' not found")
    
    def get_job_config(self, job_name: str) -> Dict[str, Any]:
        """
        Get ETL job configuration by name.
        
        Args:
            job_name: Name of the ETL job
            
        Returns:
            Dict: Job configuration
        """
        job_configs = self._configs.get('etl_jobs', {})
        
        # Try different possible keys
        for key in ['oracle_etl_jobs', 'etl_jobs', 'jobs']:
            if key in job_configs and job_name in job_configs[key]:
                config = job_configs[key][job_name].copy()
                
                # Apply global settings if available
                if 'global_settings' in job_configs:
                    global_settings = job_configs['global_settings'].copy()
                    # Don't overwrite job-specific settings
                    for k, v in global_settings.items():
                        if k not in config:
                            config[k] = v
                
                return config
        
        raise ValueError(f"Job configuration '{job_name}' not found")
    
    def get_transformation_config(self, table_name: str) -> Dict[str, Any]:
        """
        Get transformation configuration for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict: Transformation configuration
        """
        transform_configs = self._configs.get('transformation', {})
        
        # Look for table-specific transformations
        for key in ['oracle_transformation_rules', 'transformation_rules', 'transformations']:
            if key in transform_configs:
                transformations = transform_configs[key]
                for transform_name, transform_config in transformations.items():
                    if transform_config.get('table_name') == table_name:
                        return transform_config
        
        return {}
    
    def _resolve_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve environment variables in configuration values.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dict: Configuration with resolved environment variables
        """
        def resolve_value(value):
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                env_value = os.getenv(env_var)
                if env_value is None:
                    self.logger.warning(f"Environment variable {env_var} not found")
                return env_value or value
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(item) for item in value]
            else:
                return value
        
        return resolve_value(config)
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """
        Get configuration by type.
        
        Args:
            config_type: Type of configuration
            
        Returns:
            Dict: Configuration
        """
        return self._configs.get(config_type, {})
    
    def reload_configs(self):
        """Reload all configuration files."""
        self._configs.clear()
        self._load_all_configs()
        self.logger.info("All configurations reloaded")
    
    def validate_database_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate database configuration.
        
        Args:
            config: Database configuration
            
        Returns:
            bool: True if valid
        """
        required_fields = ['host', 'username', 'password']
        
        for field in required_fields:
            if field not in config:
                self.logger.error(f"Missing required database config field: {field}")
                return False
        
        # Validate port
        if 'port' in config:
            try:
                port = int(config['port'])
                if port <= 0 or port > 65535:
                    self.logger.error(f"Invalid port number: {port}")
                    return False
            except ValueError:
                self.logger.error(f"Port must be a number: {config['port']}")
                return False
        
        # Validate either service_name or sid
        if not config.get('service_name') and not config.get('sid'):
            self.logger.error("Either service_name or sid must be provided")
            return False
        
        return True
    
    def update_config(self, config_type: str, updates: Dict[str, Any]):
        """
        Update configuration in memory.
        
        Args:
            config_type: Type of configuration to update
            updates: Configuration updates
        """
        if config_type not in self._configs:
            self._configs[config_type] = {}
        
        self._configs[config_type].update(updates)
        self.logger.info(f"Updated {config_type} configuration")
    
    def save_config(self, config_type: str, file_path: Optional[Path] = None):
        """
        Save configuration to file.
        
        Args:
            config_type: Type of configuration to save
            file_path: Path to save file (optional)
        """
        if config_type not in self._configs:
            raise ValueError(f"Configuration type '{config_type}' not found")
        
        if file_path is None:
            config_files = {
                'database': 'database_configs.yml',
                'etl_jobs': 'etl_jobs.yml',
                'logging': 'logging_config.yml',
                'schedule': 'schedule_config.yml',
                'transformation': 'transformation_rules.yml'
            }
            
            if config_type not in config_files:
                raise ValueError(f"Unknown config type: {config_type}")
            
            file_path = self.config_dir / config_files[config_type]
        
        with open(file_path, 'w') as f:
            yaml.dump(self._configs[config_type], f, default_flow_style=False)
        
        self.logger.info(f"Saved {config_type} configuration to {file_path}")


# Global config manager instance
_config_manager = None


def get_config_manager() -> ConfigManager:
    """Get the global configuration manager instance."""
    global _config_manager
    
    if _config_manager is None:
        _config_manager = ConfigManager()
    
    return _config_manager