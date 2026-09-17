# Monthly dataset access through Google Forms

The existing Google Drive dataset folder stays at the same URL. A school-owned
Apps Script grants each consenting respondent's **collected Google account email**
a `reader` permission that expires at the first instant of the following month,
**00:00 UTC** (09:00 Korea time). For example, a September 30 submission expires
on October 1, not 30 days after submission. Google Drive enforces expiration;
access does not depend on the cleanup trigger running exactly at midnight.

The Form must use **Collect email addresses → Verified**, permit external Google
accounts, and allow repeated submissions. The separate free-text "Email address"
question is never used for authorization. FormApp exposes whether email collection
is enabled but does not expose Verified versus Responder input; keep Verified
enabled and review this setting when editing the Form. Every grant also requires
the existing required Terms of Use checkbox's `Agree` response.

## Components

- `Code.gs`: school-owned standalone Apps Script, with an installed Form-submit
  trigger and a five-minute retry/reconciliation trigger.
- `appsscript.json`: V8, UTC, advanced Drive API v3, and explicit OAuth scopes.
- Script Properties: activation cutoff, enabled flag, processed response IDs,
  automation-owned permission records, and the previous confirmation message.
- Existing Batch uploader: unchanged; new monthly ZIPs inherit folder access.

No file copies, URL rotation, new AWS resources, public webhook, notification email,
or change to the AWS uploader's `drive.file` refresh token is required. Downloaded
copies cannot be revoked. A forwarded Drive URL alone gives no access to another
account. The owner and `ddpslab@hanyang.ac.kr` retain their administrative access.

## Install and authorize once

1. Sign in to Apps Script as `spotlake@hanyang.ac.kr`; create a standalone project
   named **SpotLake Monthly Dataset Access**. An Apps Script default Cloud project
   is sufficient; no separately created GCP project or API-executable deployment
   is needed for installed triggers.
2. Copy `Code.gs` into the editor. In Project Settings, enable displaying
   `appsscript.json`, and replace it with this directory's manifest. Alternatively,
   enable the advanced **Drive API v3** under Services and use inferred scopes;
   all date arithmetic explicitly uses UTC.
3. Review the fixed Form/folder IDs, school owner, administrator allowlist and
   exact consent question title in `CONFIG`.
4. Verify the Form's email collection is **Verified**, the Terms checkbox is
   required, and **Limit to 1 response** is off. Do not delete historical responses.
5. Run `preflight`. Approve the script as the school account when Google asks.
   This script uses the Forms, Drive, trigger-management and account-email scopes.
   Its Drive scope is **full Drive**, unlike the separate AWS upload app's
   `drive.file` scope, so it can audit manually uploaded README/converter files and
   revoke independently assigned child-file sharing. Only school/lab administrators
   should edit this script. The code confines permission changes to the configured
   dataset tree and its grants; it does not store or export OAuth tokens.
6. Run `validateTemporaryExpiry`. It creates an empty temporary folder, grants the
   lab collaborator an expiring reader permission without sending email, checks it,
   and deletes the temporary folder. The production dataset is not changed.
7. Review other Apps Script projects' triggers for this Form to ensure no old
   automation is granting permanent/public access.
8. Run `activate` once. It audits every dataset item before mutation, rejects
   unknown collaborators, installs both triggers, removes general `anyone`/`domain`
   sharing from the dataset tree, and verifies removal. It then updates the Form
   confirmation message, records the cutoff and enables processing. Source ZIPs,
   README, converter, lab collaborators, and historical Form responses are retained.
9. Run `inspectStatus`, then use a real test submission from a non-administrator
   Google account. Verify a `reader` permission, next-month UTC expiration and
   authorized folder access. Verify a signed-out/ungranted account is denied,
   including an old direct ZIP URL. Do not infer complete access control solely
   from the top-level folder ACL.

Only submissions at or after activation are processed. Activation does not import
old responses. If activation stops during ACL cleanup, already removed public
permissions stay removed; fix the reported issue and rerun `activate`. This fails
closed rather than reopening public data. A cutover timestamp is preserved across
retries. Drive can briefly return inherited child permissions after the parent
permission was removed; attempting to delete that inherited entry returns an
error. Wait for propagation, rerun `preflight`, and retry `activate` only if it
has not completed. Do not enable limited access on vendor subfolders to suppress
this error, because that would prevent respondents from inheriting root access.
No monthly reset is added to the AWS EventBridge rule: permission expiry
already enforces the exact UTC boundary independently of the uploader.

## Runtime, retry and renewal

The installed submission trigger normally grants access shortly after submission;
the confirmation page cannot wait for it. The message therefore asks users to allow
up to five minutes. This is a normal retry interval, not an availability guarantee
during outages or quota exhaustion.

All permission writes are serialized by a script lock. Intent is saved before a
grant; an ambiguous API result is recovered by looking up the existing user grant.
No email is sent (`sendNotificationEmail: false`). Successfully handled response
IDs are skipped. Reconciliation retries current-month, post-cutover responses,
including missed submit triggers. A response from an earlier month can never extend
access into a new month. New submissions renew only automation-managed readers;
manual writer/owner permissions are protected. Expired managed readers and old
processing records are removed during reconciliation. A manually changed grant
requires review rather than being silently deleted.

Apps Script Executions and its built-in trigger failure reporting show errors.
`inspectStatus` reports the last reconciliation time without respondent emails.
Failed responses remain eligible for retry; do not continuously rerun `activate`.
The response store and Apps Script quotas are appropriate for the current small
request volume, not an unlimited subscriber service. Review quotas/storage and Drive
sharing limits before substantial growth. The initial tree audit deliberately
stops above 250 items, and each reconciliation stops processing after four minutes.

## Maintenance and recovery

- Do not restore "Anyone with the link" on the root or children; it bypasses expiry.
- Do not independently share individual ZIPs with respondents; grants belong on
  the root and must be managed by this script.
- `pauseAccess` stops new grants/retries without reopening public access. Already
  granted permissions still expire at their recorded times.
- Preserve Script Properties when updating code. Lost state cannot be reconstructed
  by blindly importing every historical response. Audit existing grants first.
- Keep the deployed script synchronized with the reviewed repository code manually.
  There is no automatic Apps Script deployment on GitHub merge and no new ECR build.
- Running users and triggers must remain the school owner; do not transfer the
  script to an outside student account.

Run offline failure-path tests with:

```sh
node --test utility/drive_form_access/access.test.cjs
```

## References

- [Installed triggers](https://developers.google.com/apps-script/guides/triggers/installable)
- [Drive permission expiration](https://developers.google.com/workspace/drive/api/reference/rest/v3/permissions)
- [Forms verified email collection](https://support.google.com/docs/answer/139706)
- [Apps Script default Cloud projects](https://developers.google.com/apps-script/guides/cloud-platform-projects)
