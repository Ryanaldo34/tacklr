// Package docs is the Docs v1 JSON the vfs adapter reads and writes.
package docs

// Document is documents.get with includeTabsContent: the tabs, or (a
// document read without tabs) the one body.
type Document struct {
	DocumentID    string                  `json:"documentId,omitempty"`
	RevisionID    string                  `json:"revisionId,omitempty"`
	Title         string                  `json:"title,omitempty"`
	Body          *Body                   `json:"body,omitempty"`
	Lists         map[string]List         `json:"lists,omitempty"`
	InlineObjects map[string]InlineObject `json:"inlineObjects,omitempty"`
	Tabs          []*Tab                  `json:"tabs,omitempty"`
}

// Tab is one document tab and its child tabs.
type Tab struct {
	TabProperties *TabProperties `json:"tabProperties,omitempty"`
	DocumentTab   *DocumentTab   `json:"documentTab,omitempty"`
	ChildTabs     []*Tab         `json:"childTabs,omitempty"`
}

// TabProperties names a tab.
type TabProperties struct {
	TabID string `json:"tabId,omitempty"`
	Title string `json:"title,omitempty"`
	Index int64  `json:"index,omitempty"`
}

// DocumentTab is a tab's content.
type DocumentTab struct {
	Body          *Body                   `json:"body,omitempty"`
	Lists         map[string]List         `json:"lists,omitempty"`
	InlineObjects map[string]InlineObject `json:"inlineObjects,omitempty"`
}

// Body is a tab's structural elements.
type Body struct {
	Content []*StructuralElement `json:"content,omitempty"`
}

// StructuralElement is one paragraph, table, section break or table of
// contents.
type StructuralElement struct {
	StartIndex      int64            `json:"startIndex,omitempty"`
	EndIndex        int64            `json:"endIndex,omitempty"`
	Paragraph       *Paragraph       `json:"paragraph,omitempty"`
	SectionBreak    *SectionBreak    `json:"sectionBreak,omitempty"`
	TableOfContents *TableOfContents `json:"tableOfContents,omitempty"`
	Table           *Table           `json:"table,omitempty"`
}

// SectionBreak marks a section break; the adapter reads only its span.
type SectionBreak struct{}

// TableOfContents marks a table of contents; the adapter reads only its
// span.
type TableOfContents struct{}

// Table is a table's rows.
type Table struct {
	TableRows []*TableRow `json:"tableRows,omitempty"`
}

// TableRow is a row's cells.
type TableRow struct {
	TableCells []*TableCell `json:"tableCells,omitempty"`
}

// TableCell is a cell's content.
type TableCell struct {
	StartIndex int64                `json:"startIndex,omitempty"`
	EndIndex   int64                `json:"endIndex,omitempty"`
	Content    []*StructuralElement `json:"content,omitempty"`
}

// Paragraph is a paragraph's runs and style.
type Paragraph struct {
	Elements       []*ParagraphElement `json:"elements,omitempty"`
	ParagraphStyle *ParagraphStyle     `json:"paragraphStyle,omitempty"`
	Bullet         *Bullet             `json:"bullet,omitempty"`
}

// ParagraphElement is a text run or an inline object.
type ParagraphElement struct {
	StartIndex          int64                `json:"startIndex,omitempty"`
	EndIndex            int64                `json:"endIndex,omitempty"`
	TextRun             *TextRun             `json:"textRun,omitempty"`
	InlineObjectElement *InlineObjectElement `json:"inlineObjectElement,omitempty"`
}

// TextRun is styled text.
type TextRun struct {
	Content   string     `json:"content,omitempty"`
	TextStyle *TextStyle `json:"textStyle,omitempty"`
}

// TextStyle is the run style the adapter reads and sets. Every field is
// sent, so an update with these fields clears the ones that are off.
type TextStyle struct {
	Bold          bool  `json:"bold"`
	Italic        bool  `json:"italic"`
	Strikethrough bool  `json:"strikethrough"`
	Link          *Link `json:"link"`
}

// Link is a hyperlink target.
type Link struct {
	URL string `json:"url,omitempty"`
}

// ParagraphStyle is a paragraph's named style.
type ParagraphStyle struct {
	NamedStyleType string `json:"namedStyleType,omitempty"`
}

// Bullet puts a paragraph in a list.
type Bullet struct {
	ListID       string `json:"listId,omitempty"`
	NestingLevel int64  `json:"nestingLevel,omitempty"`
}

// List is a list's properties.
type List struct {
	ListProperties *ListProperties `json:"listProperties,omitempty"`
}

// ListProperties is a list's levels.
type ListProperties struct {
	NestingLevels []*NestingLevel `json:"nestingLevels,omitempty"`
}

// NestingLevel is one list level.
type NestingLevel struct {
	GlyphType string `json:"glyphType,omitempty"`
}

// InlineObject is an object a paragraph embeds.
type InlineObject struct {
	InlineObjectProperties *InlineObjectProperties `json:"inlineObjectProperties,omitempty"`
}

// InlineObjectProperties holds the embedded object.
type InlineObjectProperties struct {
	EmbeddedObject *EmbeddedObject `json:"embeddedObject,omitempty"`
}

// EmbeddedObject is an embedded image.
type EmbeddedObject struct {
	Title           string           `json:"title,omitempty"`
	ImageProperties *ImageProperties `json:"imageProperties,omitempty"`
}

// ImageProperties locates an image.
type ImageProperties struct {
	ContentURI string `json:"contentUri,omitempty"`
}

// InlineObjectElement places an inline object.
type InlineObjectElement struct {
	InlineObjectID string `json:"inlineObjectId,omitempty"`
}

// BatchUpdateDocumentRequest is documents.batchUpdate.
type BatchUpdateDocumentRequest struct {
	Requests     []*Request    `json:"requests,omitempty"`
	WriteControl *WriteControl `json:"writeControl,omitempty"`
}

// BatchUpdateDocumentResponse is the reply to documents.batchUpdate.
type BatchUpdateDocumentResponse struct {
	WriteControl *WriteControl `json:"writeControl,omitempty"`
}

// WriteControl pins the revision a batch applies to.
type WriteControl struct {
	RequiredRevisionID string `json:"requiredRevisionId,omitempty"`
}

// Request is one batchUpdate edit; one field is set.
type Request struct {
	DeleteContentRange     *DeleteContentRangeRequest     `json:"deleteContentRange,omitempty"`
	InsertText             *InsertTextRequest             `json:"insertText,omitempty"`
	UpdateParagraphStyle   *UpdateParagraphStyleRequest   `json:"updateParagraphStyle,omitempty"`
	CreateParagraphBullets *CreateParagraphBulletsRequest `json:"createParagraphBullets,omitempty"`
	InsertTable            *InsertTableRequest            `json:"insertTable,omitempty"`
	UpdateTextStyle        *UpdateTextStyleRequest        `json:"updateTextStyle,omitempty"`
}

// Range is a span of a tab.
type Range struct {
	StartIndex int64  `json:"startIndex,omitempty"`
	EndIndex   int64  `json:"endIndex,omitempty"`
	TabID      string `json:"tabId,omitempty"`
}

// Location is a point in a tab.
type Location struct {
	Index int64  `json:"index,omitempty"`
	TabID string `json:"tabId,omitempty"`
}

// DeleteContentRangeRequest deletes a range.
type DeleteContentRangeRequest struct {
	Range *Range `json:"range,omitempty"`
}

// InsertTextRequest inserts text.
type InsertTextRequest struct {
	Location *Location `json:"location,omitempty"`
	Text     string    `json:"text,omitempty"`
}

// UpdateParagraphStyleRequest sets the Fields of a paragraph style.
type UpdateParagraphStyleRequest struct {
	Range          *Range          `json:"range,omitempty"`
	ParagraphStyle *ParagraphStyle `json:"paragraphStyle,omitempty"`
	Fields         string          `json:"fields,omitempty"`
}

// CreateParagraphBulletsRequest makes paragraphs a list.
type CreateParagraphBulletsRequest struct {
	Range        *Range `json:"range,omitempty"`
	BulletPreset string `json:"bulletPreset,omitempty"`
}

// InsertTableRequest inserts an empty table.
type InsertTableRequest struct {
	Location *Location `json:"location,omitempty"`
	Rows     int64     `json:"rows,omitempty"`
	Columns  int64     `json:"columns,omitempty"`
}

// UpdateTextStyleRequest sets the Fields of a text style.
type UpdateTextStyleRequest struct {
	Range     *Range     `json:"range,omitempty"`
	TextStyle *TextStyle `json:"textStyle,omitempty"`
	Fields    string     `json:"fields,omitempty"`
}
