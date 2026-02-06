#!/bin/bash
# Update JWT Token in OpenMetadata YAML files
# This script replaces the jwtToken value in all YAML files under ./openmetadata

set -e

OPENMETADATA_DIR="./openmetadata"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}OpenMetadata JWT Token Updater${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check if openmetadata directory exists
if [ ! -d "$OPENMETADATA_DIR" ]; then
    echo -e "${RED}Error: Directory $OPENMETADATA_DIR not found${NC}"
    exit 1
fi

# Find YAML files with jwtToken
YAML_FILES=$(grep -l "jwtToken:" "$OPENMETADATA_DIR"/*.yaml 2>/dev/null || true)

if [ -z "$YAML_FILES" ]; then
    echo -e "${RED}Error: No YAML files with jwtToken found in $OPENMETADATA_DIR${NC}"
    exit 1
fi

echo -e "${YELLOW}Found YAML files with JWT tokens:${NC}"
for file in $YAML_FILES; do
    echo "  - $file"
done
echo ""

# Prompt for new token
echo -e "${YELLOW}Enter the new JWT token:${NC}"
read -r NEW_TOKEN

# Validate token is not empty
if [ -z "$NEW_TOKEN" ]; then
    echo -e "${RED}Error: Token cannot be empty${NC}"
    exit 1
fi

# Basic JWT format validation (should start with eyJ)
if [[ ! "$NEW_TOKEN" =~ ^eyJ ]]; then
    echo -e "${YELLOW}Warning: Token doesn't look like a JWT (should start with 'eyJ')${NC}"
    echo -n "Continue anyway? (y/N): "
    read -r CONFIRM
    if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi
fi

echo ""
echo -e "${YELLOW}Updating JWT tokens...${NC}"

# Update each file
for file in $YAML_FILES; do
    # Use sed to replace the jwtToken value
    # Match "jwtToken: " followed by any characters until end of line
    # Note: Using portable syntax for both macOS (BSD sed) and Linux (GNU sed)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS BSD sed: requires -i '' and uses different regex syntax
        sed -i '' "s|^\\([[:space:]]*jwtToken:[[:space:]]*\\).*$|\\1$NEW_TOKEN|" "$file"
    else
        # Linux GNU sed
        sed -i "s|^\\([[:space:]]*jwtToken:[[:space:]]*\\).*$|\\1$NEW_TOKEN|" "$file"
    fi

    if [ $? -eq 0 ]; then
        echo -e "  ${GREEN}Updated:${NC} $file"
    else
        echo -e "  ${RED}Failed:${NC} $file"
    fi
done

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}JWT token update complete!${NC}"
echo -e "${GREEN}========================================${NC}"
