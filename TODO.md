# TODO

- [ ] Make `Bundle` have immutable spec.
- [ ] Make `const` for the name of the container whose image is the bundle image
      in the unpack pod.
- [ ] Looks like the registry+v1 provisioner needs to manage conversion webhooks
      (and maybe other webhooks?)

       - Inject an appropriate conversion webhook config section into the CRD
       - Generate and rotate a CA and certificate
       - Mount the certificate into the webhook pod

- [ ] Add watches for release secrets?
- [ ] Do a whole bunch of preflight checks for CRD upgrades?

      - Could this be implemented as a validating webhook for better separation of concerns?
      - What does the apiserver already check for (that we don't need to check ourselves)?
      - For the extra checks we think are necessary, why doesn't the apiserver already do them?


# Questions

- Should a `Bundle` spec be immutable? If not, what happens with a
  `BundleInstance` when its underlying bundle changes? Seems like the
  `BundleInstance` would need to reconcile and be pivoted to the new  `Bundle`.
  Regardless, the referenced `Bundle` is the desired state.

- If a `Bundle` _is_ immutable, does it accept image tags? If so, does it look
  up the digest for that tag and pin the unpacked bundle content to the SHA? If
  not, what happens with a `BundleInstance` when its underlying bundle's tag is
  updated?

