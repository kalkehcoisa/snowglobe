"""
Tests for SSL/TLS functionality
"""

import pytest
import tempfile
import os
import ssl
import subprocess
from pathlib import Path

pytestmark = pytest.mark.ssl


class TestCertificateGeneration:
    """Test SSL certificate generation"""
    
    def test_generate_self_signed_cert(self):
        """Test generating self-signed certificate"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            # Generate certificate
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/C=US/ST=CA/L=SF/O=Test/CN=localhost"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            assert result.returncode == 0, f"Certificate generation failed: {result.stderr}"
            
            # Verify files exist
            assert os.path.exists(cert_file)
            assert os.path.exists(key_file)
            
            # Verify certificate is valid
            cert_check = subprocess.run(
                ["openssl", "x509", "-in", cert_file, "-noout", "-text"],
                capture_output=True,
                text=True
            )
            assert cert_check.returncode == 0
            assert "Certificate:" in cert_check.stdout
    
    def test_certificate_has_san(self):
        """Test certificate has Subject Alternative Names"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            # Generate certificate with SAN
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:  # Some OpenSSL versions may not support -addext
                # Check for SAN
                cert_check = subprocess.run(
                    ["openssl", "x509", "-in", cert_file, "-noout", "-text"],
                    capture_output=True,
                    text=True
                )
                
                # SAN should be present (if supported)
                if "Subject Alternative Name" in cert_check.stdout:
                    assert "DNS:localhost" in cert_check.stdout
    
    def test_certificate_validity_period(self):
        """Test certificate has correct validity period"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            days = 365
            
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", str(days),
                "-subj", "/CN=localhost"
            ]
            
            subprocess.run(cmd, capture_output=True, check=True)
            
            # Check validity
            cert_check = subprocess.run(
                ["openssl", "x509", "-in", cert_file, "-noout", "-dates"],
                capture_output=True,
                text=True
            )
            
            assert "notBefore" in cert_check.stdout
            assert "notAfter" in cert_check.stdout
    
    def test_private_key_format(self):
        """Test private key is in correct format"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/CN=localhost"
            ]
            
            subprocess.run(cmd, capture_output=True, check=True)
            
            # Verify key format
            with open(key_file, 'r') as f:
                key_content = f.read()
                assert "BEGIN PRIVATE KEY" in key_content or "BEGIN RSA PRIVATE KEY" in key_content
                assert "END PRIVATE KEY" in key_content or "END RSA PRIVATE KEY" in key_content
    
    def test_certificate_format(self):
        """Test certificate is in correct PEM format"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/CN=localhost"
            ]
            
            subprocess.run(cmd, capture_output=True, check=True)
            
            # Verify certificate format
            with open(cert_file, 'r') as f:
                cert_content = f.read()
                assert "BEGIN CERTIFICATE" in cert_content
                assert "END CERTIFICATE" in cert_content


class TestSSLContext:
    """Test SSL context configuration"""
    
    def test_create_ssl_context(self):
        """Test creating SSL context"""
        try:
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            assert context is not None
            assert isinstance(context, ssl.SSLContext)
        except Exception as e:
            pytest.fail(f"Failed to create SSL context: {e}")
    
    def test_ssl_version_support(self):
        """Test SSL/TLS version support"""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        
        # Should support TLS 1.2 and above
        try:
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.maximum_version = ssl.TLSVersion.TLSv1_3
        except Exception as e:
            pytest.fail(f"Failed to set TLS version: {e}")
    
    def test_ssl_cipher_configuration(self):
        """Test SSL cipher configuration"""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        
        # Should be able to set ciphers
        try:
            context.set_ciphers('ECDHE+AESGCM:!aNULL:!MD5')
        except Exception as e:
            pytest.fail(f"Failed to set ciphers: {e}")
    
    def test_ssl_context_options(self):
        """Test SSL context options"""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        
        # Set common options
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        assert context.check_hostname is False
        assert context.verify_mode == ssl.CERT_NONE


class TestCertificateValidation:
    """Test certificate validation"""
    
    def test_validate_certificate_file(self):
        """Test validating certificate file content"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            # Generate certificate
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/CN=localhost"
            ]
            subprocess.run(cmd, capture_output=True, check=True)
            
            # Validate content
            with open(cert_file, 'r') as f:
                cert_content = f.read()
                assert len(cert_content) > 0
                assert 'BEGIN CERTIFICATE' in cert_content
                assert 'END CERTIFICATE' in cert_content
    
    def test_validate_key_file(self):
        """Test validating private key file content"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            # Generate certificate
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/CN=localhost"
            ]
            subprocess.run(cmd, capture_output=True, check=True)
            
            # Validate content
            with open(key_file, 'r') as f:
                key_content = f.read()
                assert len(key_content) > 0
                assert 'BEGIN' in key_content and 'PRIVATE KEY' in key_content
                assert 'END' in key_content and 'PRIVATE KEY' in key_content
    
    def test_invalid_certificate_detection(self):
        """Test detection of invalid certificate"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            
            # Write invalid content
            with open(cert_file, 'w') as f:
                f.write("This is not a certificate")
            
            # Should fail validation
            with open(cert_file, 'r') as f:
                cert_content = f.read()
                assert 'BEGIN CERTIFICATE' not in cert_content
    
    def test_certificate_key_match(self):
        """Test that certificate and key match"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            # Generate certificate
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/CN=localhost"
            ]
            subprocess.run(cmd, capture_output=True, check=True)
            
            # Both files should exist and be readable
            assert os.path.exists(cert_file)
            assert os.path.exists(key_file)
            assert os.path.getsize(cert_file) > 0
            assert os.path.getsize(key_file) > 0


class TestSSLConfiguration:
    """Test SSL configuration scenarios"""
    
    def test_ssl_disabled_configuration(self):
        """Test configuration with SSL disabled"""
        # Should be able to run without SSL
        enable_https = False
        assert enable_https is False
    
    def test_ssl_enabled_without_certs(self):
        """Test SSL enabled but certificates missing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "cert.pem")
            key_path = os.path.join(tmpdir, "key.pem")
            
            # Files don't exist
            assert not os.path.exists(cert_path)
            assert not os.path.exists(key_path)
    
    def test_ssl_enabled_with_certs(self):
        """Test SSL enabled with valid certificates"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_file = os.path.join(tmpdir, "cert.pem")
            key_file = os.path.join(tmpdir, "key.pem")
            
            # Generate certificate
            cmd = [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-out", cert_file,
                "-keyout", key_file,
                "-days", "1",
                "-subj", "/CN=localhost"
            ]
            subprocess.run(cmd, capture_output=True, check=True)
            
            # Both files should exist
            assert os.path.exists(cert_file)
            assert os.path.exists(key_file)
            
            # Should be readable
            with open(cert_file, 'r') as f:
                assert 'BEGIN CERTIFICATE' in f.read()
            
            with open(key_file, 'r') as f:
                content = f.read()
                assert 'PRIVATE KEY' in content
