
branch := `git rev-parse --abbrev-ref HEAD`
commit-and-push: make-commitmsg push

make-commitmsg:
  git diff --staged | sgpt --role gitcommit | tee /tmp/.commitmsg 
  git commit -F /tmp/.commitmsg

push: 
  git push origin {{branch}}

run SCRIPT_NAME:
  python wherescape.py host_scripts/{{SCRIPT_NAME}}