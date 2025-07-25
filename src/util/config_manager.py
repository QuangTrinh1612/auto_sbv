"""
Configuration management utility for ETL Framework
Handles loading, validation, and management of configuration files
"""

import os
import yaml
import json
import re
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
import threading
from copy import deepcopy


@dataclass
class ConfigSource:
    """Configuration source information"""
    file_path: str
    last_modified: datetime
    checksum: str
    environment: str


class ConfigurationError(Exception):
    """Configuration-related errors"""
    pass


class ConfigManager:
    """Centralized configuration manager for ETL Framework"""
    
    def __init__(self, config_dir: str = "./config", environment: Optional[str] = None):
        self.config_dir = Path(config_dir)
        self.environment = environment or os.getenv('ETL_ENVIRONMENT', 'development')
        self._configs: Dict[str, Any] = {}
        self._sources: Dict[str, ConfigSource] = {}
        self._lock = threading.Lock()
        self._env_pattern = re.compile(r'\$\{([^}]+)\}')
        
        # Ensure config directory exists
        self.config_dir.mkdir(parents=True, exist_ok=True)
    
    def load_config(self, config_name: str, required: bool = True) -> Dict[str, Any]:
        """Load configuration from file with environment variable substitution"""
        with self._lock:
            config_file = self._find_config_file(config_name)
            
            if not config_file and required:
                raise ConfigurationError(f"Required configuration '{config_name}' not found")
            elif not config_file:
                return {}
            
            try:
                # Load raw configuration
                raw_config = self._load_raw_config(config_file)
                
                # Apply environment-specific overrides
                processed_config = self._apply_environment_overrides(raw_config)
                
                # Substitute environment variables
                final_config = self._substitute_environment_variables(processed_config)
                
                # Validate configuration
                self._validate_config(config_name, final_config)
                
                # Cache configuration
                self._configs[config_name] = final_config
                self._sources[config_name] = ConfigSource(
                    file_path=str(config_file),
                    last_modified=datetime.fromtimestamp(config_file.stat().st_mtime),
                    checksum=self._calculate_checksum(config_file),
                    environment=self.environment
                )
                
                return final_config
                
            except Exception as e:
                raise ConfigurationError(f"Error loading configuration '{config_name}': {str(e)}")
    
    def get_config(self, config_name: str, reload: bool = False) -> Dict[str, Any]:
        """Get cached configuration or load if not cached"""
        if reload or config_name not in self._configs:
            return self.load_config(config_name)
        
        # Check if config file has been modified
        if self._is_config_modified(config_name):
            return self.load_config(config_name)
        
        return deepcopy(self._configs[config_name])
    
    def get_database_config(self, source_name: str) -> Dict[str, Any]:
        """Get database configuration for specific source"""
        db_config = self.get_config('database_configs')
        
        # Check target databases
        if source_name in db_config.get('target', {}):
            return db_config['target'][source_name]
        
        # Check source databases
        if source_name in db_config.get('sources', {}):
            return db_config['sources'][source_name]
        
        raise ConfigurationError(f"Database configuration for '{source_name}' not found")
    
    def get_job_config(self, job_name: str) -> Dict[str, Any]:
        """Get ETL job configuration"""
        job_configs = self.get_config('etl_jobs')
        
        if job_name not in job_configs.get('jobs', {}):
            raise ConfigurationError(f"Job configuration for '{job_name}' not found")
        
        job_config = job_configs['jobs'][job_name]
        
        # Apply global defaults
        if 'defaults' in job_configs:
            job_config = self._merge_configs(job_configs['defaults'], job_config)
        
        return job_config
    
    def get_transformation_rules(self, table_name: str) -> Dict[str, Any]:
        """Get transformation rules for specific table"""
        transformation_config = self.get_config('transformation_rules')
        
        # Look for table-specific rules
        if table_name in transformation_config.get('tables', {}):
            return transformation_config['tables'][table_name]
        
        # Return global rules if no table-specific rules found
        return transformation_config.get('global', {})
    
    def get_schedule_config(self, job_name: str) -> Dict[str, Any]:
        """Get scheduling configuration for specific job"""
        schedule_config = self.get_config('schedule_config')
        
        if job_name in schedule_config.get('jobs', {}):
            return schedule_config['jobs'][job_name]
        
        # Return default schedule if no job-specific schedule found
        return schedule_config.get('default', {})
    
    def validate_all_configs(self) -> Dict[str, List[str]]:
        """Validate all configuration files and return any errors"""
        validation_results = {}
        
        config_files = [
            'database_configs',
            'etl_jobs', 
            'transformation_rules',
            'schedule_config',
            'logging_config'
        ]
        
        for config_name in config_files:
            try:
                self.load_config(config_name, required=False)
                validation_results[config_name] = []
            except ConfigurationError as e:
                validation_results[config_name] = [str(e)]
        
        return validation_results
    
    def reload_all_configs(self) -> None:
        """Reload all cached configurations"""
        with self._lock:
            config_names = list(self._configs.keys())
            for config_name in config_names:
                try:
                    self.load_config(config_name)
                except ConfigurationError:
                    # Remove invalid config from cache
                    del self._configs[config_name]
                    if config_name in self._sources:
                        del self._sources[config_name]
    
    def get_config_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all loaded configurations"""
        status = {}
        
        for config_name, source in self._sources.items():
            status[config_name] = {
                'file_path': source.file_path,
                'last_modified': source.last_modified.isoformat(),
                'environment': source.environment,
                'is_current': not self._is_config_modified(config_name)
            }
        
        return status
    
    def _find_config_file(self, config_name: str) -> Optional[Path]:
        """Find configuration file with environment-specific overrides"""
        # Try environment-specific file first
        env_file = self.config_dir / f"{config_name}.{self.environment}.yml"
        if env_file.exists():
            return env_file
        
        # Try YAML format
        yaml_file = self.config_dir / f"{config_name}.yml"
        if yaml_file.exists():
            return yaml_file
        
        # Try JSON format
        json_file = self.config_dir / f"{config_name}.json"
        if json_file.exists():
            return json_file
        
        return None
    
    def _load_raw_config(self, config_file: Path) -> Dict[str, Any]:
        """Load raw configuration from file"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                if config_file.suffix.lower() in ['.yml', '.yaml']:
                    return yaml.safe_load(f) or {}
                elif config_file.suffix.lower() == '.json':
                    return json.load(f) or {}
                else:
                    raise ConfigurationError(f"Unsupported configuration file format: {config_file.suffix}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"YAML parsing error in {config_file}: {str(e)}")
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"JSON parsing error in {config_file}: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Error reading {config_file}: {str(e)}")
    
    def _apply_environment_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment-specific configuration overrides"""
        if 'environments' not in config:
            return config
        
        env_overrides = config.get('environments', {}).get(self.environment, {})
        if not env_overrides:
            return config
        
        # Deep merge environment overrides
        result = deepcopy(config)
        result = self._merge_configs(result, env_overrides)
        
        # Remove environments section from final config
        if 'environments' in result:
            del result['environments']
        
        return result
    
    def _substitute_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute environment variables in configuration values"""
        def substitute_value(value):
            if isinstance(value, str):
                return self._substitute_env_vars_in_string(value)
            elif isinstance(value, dict):
                return {k: substitute_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(item) for item in value]
            else:
                return value
        
        return substitute_value(config)
    
    def _substitute_env_vars_in_string(self, value: str) -> str:
        """Substitute environment variables in a string"""
        def replace_env_var(match):
            env_var = match.group(1)
            
            # Handle default values (e.g., ${VAR:default})
            if ':' in env_var:
                var_name, default_value = env_var.split(':', 1)
                return os.getenv(var_name, default_value)
            else:
                env_value = os.getenv(env_var)
                if env_value is None:
                    raise ConfigurationError(f"Environment variable '{env_var}' not found")
                return env_value
        
        return self._env_pattern.sub(replace_env_var, value)
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two configuration dictionaries"""
        result = deepcopy(base)
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = deepcopy(value)
        
        return result
    
    def _validate_config(self, config_name: str, config: Dict[str, Any]) -> None:
        """Validate configuration based on config type"""
        validators = {
            'database_configs': self._validate_database_config,
            'etl_jobs': self._validate_job_config,
            'transformation_rules': self._validate_transformation_config,
            'schedule_config': self._validate_schedule_config,
            'logging_config': self._validate_logging_config
        }
        
        validator = validators.get(config_name)
        if validator:
            validator(config)
    
    def _validate_database_config(self, config: Dict[str, Any]) -> None:
        """Validate database configuration"""
        # Validate target configuration
        if 'target' not in config:
            raise ConfigurationError("Database config must have 'target' section")
        
        for db_name, db_config in config['target'].items():
            self._validate_single_database_config(db_name, db_config)
        
        # Validate source configurations
        if 'sources' in config:
            for db_name, db_config in config['sources'].items():
                self._validate_single_database_config(db_name, db_config)
    
    def _validate_single_database_config(self, db_name: str, db_config: Dict[str, Any]) -> None:
        """Validate single database configuration"""
        required_fields = ['type', 'connection']
        for field in required_fields:
            if field not in db_config:
                raise ConfigurationError(f"Database '{db_name}' missing required field: {field}")
        
        # Validate connection parameters based on database type
        db_type = db_config['type'].lower()
        connection = db_config['connection']
        
        if db_type == 'sqlserver':
            required_conn_fields = ['server', 'database']
            for field in required_conn_fields:
                if field not in connection:
                    raise ConfigurationError(f"SQL Server '{db_name}' missing required connection field: {field}")
        
        elif db_type == 'mysql':
            required_conn_fields = ['host', 'database']
            for field in required_conn_fields:
                if field not in connection:
                    raise ConfigurationError(f"MySQL '{db_name}' missing required connection field: {field}")
        
        elif db_type == 'postgresql':
            required_conn_fields = ['host', 'database']
            for field in required_conn_fields:
                if field not in connection:
                    raise ConfigurationError(f"PostgreSQL '{db_name}' missing required connection field: {field}")
        
        elif db_type == 'oracle':
            # Oracle can use either host/port/service_name or tns_alias
            has_host_config = all(field in connection for field in ['host', 'service_name'])
            has_tns_config = 'tns_alias' in connection
            
            if not (has_host_config or has_tns_config):
                raise ConfigurationError(f"Oracle '{db_name}' must have either host/service_name or tns_alias")
    
    def _validate_job_config(self, config: Dict[str, Any]) -> None:
        """Validate ETL job configuration"""
        if 'jobs' not in config:
            raise ConfigurationError("Job config must have 'jobs' section")
        
        for job_name, job_config in config['jobs'].items():
            required_fields = ['source', 'target', 'extraction']
            for field in required_fields:
                if field not in job_config:
                    raise ConfigurationError(f"Job '{job_name}' missing required field: {field}")
    
    def _validate_transformation_config(self, config: Dict[str, Any]) -> None:
        """Validate transformation rules configuration"""
        # Basic validation - transformation rules can be flexible
        if not isinstance(config, dict):
            raise ConfigurationError("Transformation config must be a dictionary")
    
    def _validate_schedule_config(self, config: Dict[str, Any]) -> None:
        """Validate schedule configuration"""
        if 'jobs' in config:
            for job_name, schedule in config['jobs'].items():
                if 'cron' not in schedule and 'interval' not in schedule:
                    raise ConfigurationError(f"Schedule for job '{job_name}' must have either 'cron' or 'interval'")
    
    def _validate_logging_config(self, config: Dict[str, Any]) -> None:
        """Validate logging configuration"""
        # Basic validation for logging config
        if not isinstance(config, dict):
            raise ConfigurationError("Logging config must be a dictionary")
    
    def _is_config_modified(self, config_name: str) -> bool:
        """Check if configuration file has been modified since last load"""
        if config_name not in self._sources:
            return True
        
        source = self._sources[config_name]
        config_file = Path(source.file_path)
        
        if not config_file.exists():
            return True
        
        current_modified = datetime.fromtimestamp(config_file.stat().st_mtime)
        return current_modified > source.last_modified
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate file checksum for change detection"""
        import hashlib
        
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()


class ConfigValidator:
    """Configuration validation utilities"""
    
    @staticmethod
    def validate_connection_string(connection_string: str, db_type: str) -> bool:
        """Validate database connection string format"""
        if db_type.lower() == 'sqlserver':
            # Basic SQL Server connection string validation
            required_parts = ['Server=', 'Database=']
            return all(part.lower() in connection_string.lower() for part in required_parts)
        
        # Add more database types as needed
        return True
    
    @staticmethod
    def validate_cron_expression(cron_expr: str) -> bool:
        """Validate cron expression format"""
        try:
            from croniter import croniter
            return croniter.is_valid(cron_expr)
        except ImportError:
            # Basic validation if croniter is not available
            parts = cron_expr.split()
            return len(parts) == 5
    
    @staticmethod
    def validate_environment_variables(config: Dict[str, Any]) -> List[str]:
        """Check for missing environment variables in configuration"""
        missing_vars = []
        
        def check_value(value):
            if isinstance(value, str):
                env_pattern = re.compile(r'\$\{([^}:]+)')
                for match in env_pattern.finditer(value):
                    var_name = match.group(1)
                    if not os.getenv(var_name):
                        missing_vars.append(var_name)
            elif isinstance(value, dict):
                for v in value.values():
                    check_value(v)
            elif isinstance(value, list):
                for item in value:
                    check_value(item)
        
        check_value(config)
        return missing_vars


# Singleton instance
_config_manager = None

def get_config_manager(config_dir: str = "./config", environment: Optional[str] = None) -> ConfigManager:
    """Get singleton ConfigManager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_dir, environment)
    return _config_manager


if __name__ == "__main__":
    # Example usage
    config_manager = get_config_manager()
    
    try:
        # Load database configuration
        db_config = config_manager.get_config('database_configs')
        print("Database configuration loaded successfully")
        
        # Get specific database configuration
        target_config = config_manager.get_database_config('sql_server_warehouse')
        print(f"Target database type: {target_config.get('type')}")
        
        # Validate all configurations
        validation_results = config_manager.validate_all_configs()
        for config_name, errors in validation_results.items():
            if errors:
                print(f"Validation errors in {config_name}: {errors}")
            else:
                print(f"{config_name}: Valid")
        
        # Get configuration status
        status = config_manager.get_config_status()
        for config_name, info in status.items():
            print(f"{config_name}: {info}")
            
    except ConfigurationError as e:
        print(f"Configuration error: {e}")