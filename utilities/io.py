"""
IO utilities for Gaming BI System.
This module provides basic file operations and directory management.
"""

import json
import os
from pathlib import Path
from datetime import datetime, date
from typing import Union, Dict, Any


def header(msg: str) -> None:
    """
    Print a formatted header message with underline.
    
    Args:
        msg (str): The message to display as header
    """
    print("\n")
    underline = "=" * len(msg)
    print(f"{msg}\n{underline}\n")


def read_file(path: Path) -> str:
    """
    Read content from a text file.
    
    Args:
        path (Path): Path to the file to read
        
    Returns:
        str: File content as string
        
    Raises:
        FileNotFoundError: If file doesn't exist
        IOError: If file cannot be read
    """
    with open(path, "r", encoding='utf-8') as file:
        return file.read()


def write_file(path: Path, content: str) -> None:
    """
    Write content to a text file, creating directories if needed.
    
    Args:
        path (Path): Path where to write the file
        content (str): Content to write to file
    """
    ensure_dir(path.parent)
    with open(path, "w", newline='', encoding='utf-8') as file:
        file.write(content)


def ensure_dir(path: Path) -> None:
    """
    Ensure directory exists, create if it doesn't.
    
    Args:
        path (Path): Directory path to ensure exists
    """
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Dict[str, Any]:
    """
    Read JSON file and return parsed data.
    
    Args:
        path (Path): Path to JSON file
        
    Returns:
        Dict[str, Any]: Parsed JSON data, empty dict if file doesn't exist
    """
    try:
        with open(path, "r", encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path}: {e}")


def write_json(path: Path, obj: Union[Dict, list]) -> bool:
    """
    Write object to JSON file with datetime conversion.
    
    Args:
        path (Path): Path where to write JSON file
        obj (Union[Dict, list]): Object to serialize to JSON
        
    Returns:
        bool: True if successful, False otherwise
    """
    def default_converter(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    try:
        ensure_dir(path.parent)
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(obj, file, indent=4, default=default_converter)
        return True
    except Exception as e:
        print(f"Error writing JSON file {path}: {e}")
        return False
