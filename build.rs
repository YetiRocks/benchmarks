//! UI Build Tool
//!
//! Builds the React/Vite UI programmatically by running npm commands.
//! This can be run with: cargo run --bin build-ui

use std::process::{Command, exit};

fn main() {
    let app_dir = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(e) => {
            eprintln!("Failed to get current directory: {}", e);
            exit(1);
        }
    };

    let source_dir = app_dir.join("source");
    let web_dir = app_dir.join("web");

    println!("Building UI from {:?}", source_dir);
    println!("Output directory: {:?}", web_dir);

    let npm_check = match Command::new("npm")
        .arg("--version")
        .output() {
            Ok(output) => output,
            Err(e) => {
                eprintln!("Failed to run npm. Is Node.js/npm installed? Error: {}", e);
                exit(1);
            }
        };

    if !npm_check.status.success() {
        eprintln!("npm is not available. Please install Node.js and npm.");
        exit(1);
    }

    println!("npm found: {}", String::from_utf8_lossy(&npm_check.stdout).trim());

    println!("\nInstalling npm dependencies...");
    let install_status = match Command::new("npm")
        .arg("install")
        .current_dir(&source_dir)
        .status() {
            Ok(status) => status,
            Err(e) => {
                eprintln!("Failed to run npm install: {}", e);
                exit(1);
            }
        };

    if !install_status.success() {
        eprintln!("npm install failed with status: {}", install_status);
        exit(1);
    }

    println!("Dependencies installed");

    println!("\nBuilding production UI...");
    let build_status = match Command::new("npm")
        .arg("run")
        .arg("build")
        .current_dir(&source_dir)
        .status() {
            Ok(status) => status,
            Err(e) => {
                eprintln!("Failed to run npm run build: {}", e);
                exit(1);
            }
        };

    if !build_status.success() {
        eprintln!("npm run build failed with status: {}", build_status);
        exit(1);
    }

    println!("UI built successfully at {:?}", web_dir);

    if !web_dir.exists() || !web_dir.join("index.html").exists() {
        eprintln!("Build completed but index.html not found in {:?}", web_dir);
        exit(1);
    }

    println!("\nBuild complete! UI is ready to serve.");
}
