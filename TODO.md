# TODO

- [ ] Make `Bundle` have immutable spec.
- [ ] Make `const` for the name of the container whose image is the bundle image
      in the unpack pod.

# Questions

- Should a `Bundle` spec be immutable? If not, what happens with a
  `BundleInstance` when its underlying bundle changes?

- If a `Bundle` _is_ immutable, does it accept image tags? If so, does it look
  up the digest for that tag and pin the unpacked bundle content to the SHA? If
  not, what happens with a `BundleInstance` when its underlying bundle's tag is
  updated?

