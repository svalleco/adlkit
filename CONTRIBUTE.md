# Contribution Guidelines

1. When in doubt, adhere to [PEP8](https://www.python.org/dev/peps/pep-0008/).

2. When in doubt, adhere to [Gitlab Flow](https://about.gitlab.com/2016/07/27/the-11-rules-of-gitlab-flow/).

3. Use the follow prefixes on commit messages
  - BUG: use if fixing something that is broken
  - DOC: use if adding / editing comments or updating README(s)
  - TST: use if adding / editing test cases
  - MAINT: use if something isn’t broken but might be later on
  - ENH: use if adding a new feature

4. Use the following conventions for log messages.
Refer to https://docs.python.org/2/howto/logging.html for more guidelines.
  - INFO: should be used for timing / counting purposes,
  no static statements, key=value syntax ( e.g. "meows=10" )
  - DEBUG: static and dynamic statements allowed ( e.g. “entering function”)

5. If leaving a `#TODO`, make sure to leave you username next to it with
 a descriptive explanation:
    ```python
    #TODO - wghilliard
    # Need to determine a better way to generate random number, the fair
    # dice roll method doesn't seem to provide enough entropy.
    def random_num():
        return 4
    ```

5. When in doubt, [search open issues](https://github.com/anomalousdl/adlkit/issues), then ask.

6. We reserve the right to revoke pull requests that do not follow the
described coding conventions.



