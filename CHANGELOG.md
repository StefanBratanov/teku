# Changelog

## Upcoming Breaking Changes
 - Teku will stop publishing the `-jdk26` Docker images in a future release, because Java 26 is end of life and no longer receives security fixes. Use the `-jdk25` tags instead.

## Current Releases

## Unreleased Changes

### Breaking Changes

### Additions and Improvements

### Bug Fixes
 - Docker images no longer trust the emSign ECC Root CA - G3, emSign Root CA - G1, emSign Root CA - G2 or AAA Certificate Services roots, which OpenJDK removed after the eMudhra intermediate certificate authorities were compromised in October 2026. The Eclipse Temurin base images do not include this fix yet.