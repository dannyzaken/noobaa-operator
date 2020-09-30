package cli

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/noobaa/noobaa-operator/v2/pkg/backingstore"
	"github.com/noobaa/noobaa-operator/v2/pkg/bucket"
	"github.com/noobaa/noobaa-operator/v2/pkg/bucketclass"
	"github.com/noobaa/noobaa-operator/v2/pkg/crd"
	"github.com/noobaa/noobaa-operator/v2/pkg/diagnose"
	"github.com/noobaa/noobaa-operator/v2/pkg/install"
	"github.com/noobaa/noobaa-operator/v2/pkg/obc"
	"github.com/noobaa/noobaa-operator/v2/pkg/olm"
	"github.com/noobaa/noobaa-operator/v2/pkg/operator"
	"github.com/noobaa/noobaa-operator/v2/pkg/options"
	"github.com/noobaa/noobaa-operator/v2/pkg/pvstore"
	"github.com/noobaa/noobaa-operator/v2/pkg/system"
	"github.com/noobaa/noobaa-operator/v2/pkg/util"
	"github.com/noobaa/noobaa-operator/v2/pkg/version"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

// ASCIILogo1 is an ascii logo of noobaa
const ASCIILogo1 = `
._   _            ______              
| \ | |           | ___ \             
|  \| | ___   ___ | |_/ / __ _  __ _  
| . \ |/ _ \ / _ \| ___ \/ _\ |/ _\ | 
| |\  | (_) | (_) | |_/ / (_| | (_| | 
\_| \_/\___/ \___/\____/ \__,_|\__,_| 
`

// ASCIILogo2 is an ascii logo of noobaa
const ASCIILogo2 = `
#                       # 
#    /~~\___~___/~~\    # 
#   |               |   # 
#    \~~|\     /|~~/    # 
#        \|   |/        # 
#         |   |         # 
#         \~~~/         # 
#                       # 
#      N O O B A A      # 
`

//Run runs
func Run() {
	err := Cmd().Execute()
	if err != nil {
		os.Exit(1)
	}
}

// Cmd returns a CLI command
func Cmd() *cobra.Command {

	util.InitLogger()

	rand.Seed(time.Now().UTC().UnixNano())

	logo := ASCIILogo1
	if rand.Intn(2) == 0 { // 50% chance
		logo = ASCIILogo2
	}

	// Root command
	rootCmd := &cobra.Command{
		Use:   "noobaa",
		Short: logo,
	}

	rootCmd.PersistentFlags().AddFlagSet(options.FlagSet)
	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	optionsCmd := options.Cmd()

	completionCmd := &cobra.Command{
		Use:   "completion",
		Short: "Generates bash completion scripts",
		Long: fmt.Sprintf(`
Load noobaa completion to bash:
(add to your ~/.bashrc and ~/.bash_profile to auto load)

. <(%s completion)
`, rootCmd.Name()),
		Run: func(cmd *cobra.Command, args []string) {
			alias, _ := cmd.Flags().GetString("alias")
			if alias != "" {
				rootCmd.Use = alias
			}
			err := rootCmd.GenBashCompletion(os.Stdout)
			if err != nil {
				fmt.Printf("got error on GenBashCompletion. %v", err)
			}

		},
	}
	completionCmd.Flags().String("alias", "", "Custom alias name to generate the completion for")

	groups := templates.CommandGroups{{
		Message: "Install:",
		Commands: []*cobra.Command{
			install.CmdInstall(),
			install.CmdUninstall(),
			install.CmdStatus(),
		},
	}, {
		Message: "Manage:",
		Commands: []*cobra.Command{
			backingstore.Cmd(),
			bucketclass.Cmd(),
			obc.Cmd(),
			diagnose.Cmd(),
			system.CmdUI(),
		},
	}, {
		Message: "Advanced:",
		Commands: []*cobra.Command{
			operator.Cmd(),
			system.Cmd(),
			system.CmdAPICall(),
			bucket.Cmd(),
			pvstore.Cmd(),
			crd.Cmd(),
			olm.Cmd(),
		},
	}}

	groups.Add(rootCmd)

	rootCmd.AddCommand(
		version.Cmd(),
		optionsCmd,
		completionCmd,
	)

	templates.ActsAsRootCommand(rootCmd, []string{}, groups...)
	templates.UseOptionsTemplates(optionsCmd)

	return rootCmd
}
