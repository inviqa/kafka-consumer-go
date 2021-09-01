# Releasing

We keep a changelog, powered by [GitHub Changelog Generator].

When ready to tag a release, make a new branch from the `master` branch for the changelog entries:
1. Generate a `repo` scope token for use with the changelog generator: https://github.com/settings/tokens/new?description=GitHub%20Changelog%20Generator%20token
1. Export it in your environment: `export CHANGELOG_GITHUB_TOKEN=...`
1. Run the following docker command to generate the changelog, replacing `<nextReleaseTag>` with the version number you
   wish to release:
  ```bash
  docker run -e CHANGELOG_GITHUB_TOKEN="$CHANGELOG_GITHUB_TOKEN" -it --rm -v "$(pwd)":/usr/local/src/your-app -v "$(pwd)/github-changelog-http-cache":/tmp/github-changelog-http-cache githubchangeloggenerator/github-changelog-generator --user inviqa --project kafka-consumer-go --exclude-labels "duplicate,question,invalid,wontfix,skip-changelog" --release-branch master --future-release <nextReleaseTag>
  ```
1. Examine the generated `CHANGELOG.md`, verify the changes make sense.
1. Commit the resulting `CHANGELOG.md`, push and raise a pull request with the label `skip-changelog`.
1. Once merged, continue with the release process below.

## Finalising a Release

Once the `CHANGELOG.md` is in the branch you wish to release:

1. Tag the release version with `git tag <releaseVersion>`
1. Push the tag to the repository: `git push origin <releaseVersion>`
1. Create a [new release] using the tag name and the contents of the changelog for that version.

[GitHub Changelog Generator]: https://github.com/github-changelog-generator/github-changelog-generator
[new release]: https://github.com/inviqa/kafka-consumer-go/releases
