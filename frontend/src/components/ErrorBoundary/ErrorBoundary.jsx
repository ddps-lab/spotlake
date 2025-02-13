import React, { Component } from "react";
import { Alert, Snackbar } from "@mui/material";

class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, errorMessage: "" };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, errorMessage: error.message };
  }

  componentDidCatch(error, errorInfo) {
    console.error("ErrorBoundary caught an error:", error, errorInfo);
  }

  handleClose = () => {
    this.setState({ hasError: false, errorMessage: "" });
  };

  render() {
    if (this.state.hasError) {
      return (
        <Snackbar
          open={this.state.hasError}
          anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
          autoHideDuration={5000}
          onClose={this.handleClose}
        >
          <Alert severity="error" onClose={this.handleClose}>
            {this.state.errorMessage || "Something went wrong."}
          </Alert>
        </Snackbar>
      );
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
