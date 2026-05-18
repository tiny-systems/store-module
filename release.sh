#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Function to get the latest git tag
get_latest_tag() {
    git fetch --tags 2>/dev/null || true
    git tag -l "v*" | sort -V | tail -1
}

# Function to bump version
bump_version() {
    local version=$1
    local bump_type=$2

    # Remove 'v' prefix if present
    version=${version#v}

    # Split version into components
    IFS='.' read -r major minor patch <<< "$version"

    case $bump_type in
        major)
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        minor)
            minor=$((minor + 1))
            patch=0
            ;;
        patch)
            patch=$((patch + 1))
            ;;
        *)
            error "Invalid bump type: $bump_type. Use: major, minor, or patch"
            ;;
    esac

    echo "v${major}.${minor}.${patch}"
}

# Function to update SDK to latest version
update_sdk() {
    info "Updating github.com/tiny-systems/module to latest version..."

    # Get the latest version
    go get github.com/tiny-systems/module@latest

    # Tidy up
    info "Running go mod tidy..."
    go mod tidy

    # Show the updated version
    NEW_VERSION=$(go list -m github.com/tiny-systems/module | awk '{print $2}')
    info "SDK updated to: $NEW_VERSION"

    # Check if go.mod was modified
    if [[ -n $(git status -s go.mod go.sum 2>/dev/null) ]]; then
        info "go.mod and/or go.sum have been modified"
        read -p "Commit the changes? (Y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            git add go.mod go.sum
            git commit -m "update SDK to $NEW_VERSION"
            info "Changes committed"
        else
            warn "Don't forget to commit the changes!"
        fi
    else
        info "No changes to commit (SDK already at latest version)"
    fi
}

# Main script
main() {
    # Handle 'update' command
    if [[ "${1:-}" == "update" ]]; then
        update_sdk
        exit 0
    fi
    # Check if git repo
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        error "Not a git repository"
    fi

    # Check for uncommitted changes
    if [[ -n $(git status -s) ]]; then
        warn "You have uncommitted changes:"
        git status -s
        read -p "Continue anyway? (Y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            exit 1
        fi
    fi

    # Get bump type from argument or prompt
    BUMP_TYPE=${1:-}
    if [[ -z "$BUMP_TYPE" ]]; then
        echo "Select version bump type:"
        echo "  1) patch (0.1.x -> 0.1.x+1)"
        echo "  2) minor (0.x.0 -> 0.x+1.0)"
        echo "  3) major (x.0.0 -> x+1.0.0)"
        read -p "Enter choice [1-3]: " -n 1 -r choice
        echo
        case $choice in
            1) BUMP_TYPE="patch" ;;
            2) BUMP_TYPE="minor" ;;
            3) BUMP_TYPE="major" ;;
            *) error "Invalid choice" ;;
        esac
    fi

    # Get current version
    CURRENT_TAG=$(get_latest_tag)
    if [[ -z "$CURRENT_TAG" ]]; then
        warn "No existing tags found, starting from v0.0.0"
        CURRENT_TAG="v0.0.0"
    fi

    info "Current version: $CURRENT_TAG"

    # Calculate new version
    NEW_TAG=$(bump_version "$CURRENT_TAG" "$BUMP_TYPE")
    info "New version: $NEW_TAG"

    # Ask for commit message
    read -p "Commit message (default: 'release $NEW_TAG'): " COMMIT_MSG
    COMMIT_MSG=${COMMIT_MSG:-"release $NEW_TAG"}

    # Commit any staged changes
    if [[ -n $(git diff --cached --name-only) ]]; then
        info "Committing changes..."
        git commit -m "$COMMIT_MSG"
    fi

    # Create tag locally first
    info "Creating tag $NEW_TAG..."
    git tag -a "$NEW_TAG" -m "$COMMIT_MSG"

    # Check if there are unpushed commits
    PUSH_COMMITS=false
    if [[ -n $(git log origin/$(git rev-parse --abbrev-ref HEAD)..HEAD 2>/dev/null) ]]; then
        read -p "Push commits to remote? (Y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            warn "Commits not pushed. Tag will not be pushed either (would point to non-existent commit)."
            info "✓ Tag $NEW_TAG created locally (not pushed)"
            warn "To push later: git push && git push origin $NEW_TAG"
            return
        else
            info "Pushing commits to remote..."
            git push
            PUSH_COMMITS=true
        fi
    fi

    # Now ask about pushing the tag
    read -p "Push tag to remote? (Y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        info "✓ Tag $NEW_TAG created locally (not pushed)"
        warn "Remember to push with: git push origin $NEW_TAG"
    else
        info "Pushing tag to remote..."
        git push origin "$NEW_TAG"

        info "✓ Release $NEW_TAG created and pushed successfully!"
        echo ""
        info "Tag: $NEW_TAG"
        info "You can create a GitHub release at:"
        echo "  https://github.com/$(git remote get-url origin | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/releases/new?tag=$NEW_TAG"
    fi
}

# Run main function
main "$@"
