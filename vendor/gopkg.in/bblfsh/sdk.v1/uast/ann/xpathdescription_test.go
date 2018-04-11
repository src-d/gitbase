package ann

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/bblfsh/sdk.v1/uast"
)

// go test -v -run 'TestRulesDocSuite' ./uast/ann

type RulesDocSuite struct {
	suite.Suite
}

func TestRulesDocSuite(t *testing.T) {
	suite.Run(t, new(RulesDocSuite))
}

func (suite *RulesDocSuite) TestAny() {
	rule := On(Any).Roles(uast.Identifier)
	expected := head + `| /self::\*\[\*\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestNotAny() {
	rule := On(Not(Any)).Roles(uast.Identifier)
	expected := head + `| /self::\*\[not\(\*\)\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestHasInternalType() {
	rule := On(HasInternalType("foo")).Roles(uast.Identifier)
	expected := head + `| /self::\*\[@InternalType='foo'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestHasProperty() {
	rule := On(HasProperty("key", "value")).Roles(uast.Identifier)
	expected := head + `| /self::\*\[@key\]\[@key='value'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestHasInternalRole() {
	rule := On(HasInternalRole("role")).Roles(uast.Identifier)
	expected := head + `| /self::\*\[@internalRole\]\[@internalRole='role'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestHasChild() {
	rule := On(HasChild(HasInternalType("foo"))).Roles(uast.Identifier)
	expected := head + `| /self::\*\[child::@InternalType='foo'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestToken() {
	rule := On(HasToken("foo")).Roles(uast.Identifier)
	expected := head + `| /self::\*\[@Token='foo'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestAnd() {
	rule := On(And(
		HasToken("foo"),
		HasToken("bar"),
		HasInternalType("bla"),
	)).Roles(uast.Identifier)
	expected := head +
		`| /self::\*\[\(@Token='foo'\) and \(@Token='bar'\) and \(@InternalType='bla'\)\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestOr() {
	rule := On(Or(
		HasToken("foo"),
		HasToken("bar"),
		HasInternalType("bla"),
	)).Roles(uast.Identifier)
	expected := head +
		`| /self::\*\[\(@Token='foo'\) or \(@Token='bar'\) or \(@InternalType='bla'\)\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestSelf() {
	rule := On(Any).Self(On(HasToken("foo")).Roles(uast.Identifier))
	expected := head + `| /self::\*\[@Token='foo'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)

	rule = On(Any).Self(On(HasToken("foo"))).Roles(uast.Identifier)
	expected = head + `| /self::\*\[\*\] | Identifier |` + "\n"
	obtained = rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestChildren() {
	rule := On(Any).Children(On(HasToken("foo")).Roles(uast.Identifier))
	expected := head + `| /\*\[@Token='foo'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestDescendants() {
	rule := On(Any).Descendants(On(HasToken("foo")).Roles(uast.Identifier))
	expected := head + `| //\*\[@Token='foo'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestDescendantsOrSelf() {
	rule := On(Any).DescendantsOrSelf(On(HasToken("foo")).Roles(uast.Identifier))
	expected := head + `| /descendant\-or\-self::\*\[@Token='foo'\] | Identifier |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestMisc1() {
	rule := On(Any).Self(
		On(Not(HasInternalType("FILE"))).Error(fmt.Errorf("root must be CompilationUnit")),
		On(HasInternalType("FILE")).Roles(uast.Identifier).Descendants(
			On(HasInternalType("identifier")).Roles(uast.Identifier, uast.Qualified),
			On(HasInternalType("binary expression")).Children(
				On(HasInternalType("left")).Roles(uast.Binary, uast.Left)),
		))
	expected := head +
		`| /self::\*\[not\(@InternalType='FILE'\)\] | Error |` + "\n" +
		`| /self::\*\[@InternalType='FILE'\] | Identifier |` + "\n" +
		`| /self::\*\[@InternalType='FILE'\]//\*\[@InternalType='identifier'\] | Identifier, Qualified |` + "\n" +
		`| /self::\*\[@InternalType='FILE'\]//\*\[@InternalType='binary expression'\]/\*\[@InternalType='left'\] | Binary, Left |` + "\n"
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}

func (suite *RulesDocSuite) TestMarkdownEscapes() {
	rule := On(Any).Descendants(
		On(HasInternalType(`\`)).Roles(uast.Or),
		On(HasInternalType("|")).Roles(uast.Or),
		On(HasInternalType("||")).Roles(uast.Or),
		On(HasInternalType("`")).Roles(uast.Or),
		On(HasInternalType("*")).Roles(uast.Or),
		On(HasInternalType("_")).Roles(uast.Or),
		On(HasInternalType("{")).Roles(uast.Or),
		On(HasInternalType("}")).Roles(uast.Or),
		On(HasInternalType("[")).Roles(uast.Or),
		On(HasInternalType("]")).Roles(uast.Or),
		On(HasInternalType("(")).Roles(uast.Or),
		On(HasInternalType(")")).Roles(uast.Or),
		On(HasInternalType("#")).Roles(uast.Or),
		On(HasInternalType("+")).Roles(uast.Or),
		On(HasInternalType("-")).Roles(uast.Or),
		On(HasInternalType(".")).Roles(uast.Or),
		On(HasInternalType("!")).Roles(uast.Or),
	)
	expected := head +
		`| //\*\[@InternalType='\\'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\|'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\|\|'\] | Or |` + "\n" +
		"| //\\*\\[@InternalType='`'\\] | Or |\n" +
		`| //\*\[@InternalType='\*'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\_'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\{'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\}'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\['\] | Or |` + "\n" +
		`| //\*\[@InternalType='\]'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\('\] | Or |` + "\n" +
		`| //\*\[@InternalType='\)'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\#'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\+'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\-'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\.'\] | Or |` + "\n" +
		`| //\*\[@InternalType='\!'\] | Or |` + "\n" +
		""
	obtained := rule.String()
	require.Equal(suite.T(), expected, obtained)
}
