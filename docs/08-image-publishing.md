# Publishing OpenProjectX images

[Documentation index](README.md)

The [Publish OpenProjectX image workflow](../.github/workflows/publish-openprojectx.yml)
builds this checkout and pushes `ghcr.io/openprojectx/bitmagnet` for Linux AMD64
and ARM64. It is independent of the disabled upstream workflows and runs only
in `OpenProjectX/bitmagnet`.

## Triggers and tags

- Pushes to `main`, `master`, `dev`, or `develop` publish the branch tag and
  `sha-<full commit SHA>`, plus the first eight commit characters as a short tag
  (for example, `ghcr.io/openprojectx/bitmagnet:a1b2c3d4`).
- Builds of the repository's default branch also update `latest`.
- Version tags such as `v0.10.2` publish `v0.10.2`, `v0.10`, and both commit tags.
  Prereleases receive their full version tag; they do not update `latest`.
- **Actions → Publish OpenProjectX image → Run workflow** publishes the selected
  branch or version tag. The workflow must be present on the default branch
  for the manual trigger to appear.

`latest` follows the default branch, including unreleased commits. Use a version,
commit tag, or the digest printed in the workflow summary for deployments.
Branch/version tags can move when rebuilt; the digest identifies the exact image.

## Repository setup

Push the workflow and application changes to `https://github.com/OpenProjectX/bitmagnet.git`.
Keep the original workflows disabled and allow this new workflow to run. If
Actions is disabled for the entire repository, enable Actions first.

Authentication uses the built-in `GITHUB_TOKEN` with `contents: read` and
`packages: write`; no personal access token or Docker Hub secret is needed.
The organization must allow Actions and package creation. If this package
already exists, grant this repository Actions access in the package settings.
This follows [GitHub's GHCR publishing instructions](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images).

After the first successful publication, check package visibility. Make the
package public for anonymous pulls, or configure Kubernetes image pull credentials
for a private package. Publishing does not change package visibility or deploy
anything to Kubernetes.

Example Helm values after a successful default-branch build:

```yaml
image:
  repository: ghcr.io/openprojectx/bitmagnet
  tag: latest
```

## Build behavior

The workflow uses `ci.Dockerfile`, Buildx, QEMU for ARM64 runtime-image steps,
and GitHub Actions layer caching. The Go builder cross-compiles the binary and
embeds the committed `webui/dist/bitmagnet/browser` assets. It does not regenerate
the web UI: rebuild and commit those assets when changing frontend code.

The image source label points to the OpenProjectX repository, and its version
uses the generated image tag. The original image entrypoint remains intact.
Tracker crawling stays disabled until explicitly configured; see the
[tracker integration guide](07-tracker-integration.md).
