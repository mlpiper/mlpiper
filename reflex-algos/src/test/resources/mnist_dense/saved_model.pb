св
╫ о 
9
Add
x"T
y"T
z"T"
Ttype:
2	
А
ApplyGradientDescent
var"TА

alpha"T

delta"T
out"TА"
Ttype:
2	"
use_lockingbool( 
x
Assign
ref"TА

value"T

output_ref"TА"	
Ttype"
validate_shapebool("
use_lockingbool(Ш
R
BroadcastGradientArgs
s0"T
s1"T
r0"T
r1"T"
Ttype0:
2	
8
Cast	
x"SrcT	
y"DstT"
SrcTtype"
DstTtype
h
ConcatV2
values"T*N
axis"Tidx
output"T"
Nint(0"	
Ttype"
Tidxtype0:
2	
8
Const
output"dtype"
valuetensor"
dtypetype
W

ExpandDims

input"T
dim"Tdim
output"T"	
Ttype"
Tdimtype0:
2	
4
Fill
dims

value"T
output"T"	
Ttype
>
FloorDiv
x"T
y"T
z"T"
Ttype:
2	
в
	HashTable
table_handleА"
	containerstring "
shared_namestring "!
use_node_name_sharingbool( "
	key_dtypetype"
value_dtypetypeИ
.
Identity

input"T
output"T"	
Ttype
`
InitializeTable
table_handleА
keys"Tkey
values"Tval"
Tkeytype"
Tvaltype
u
LookupTableFind
table_handleА
keys"Tin
default_value"Tout
values"Tout"
Tintype"
Touttype
o
MatMul
a"T
b"T
product"T"
transpose_abool( "
transpose_bbool( "
Ttype:

2
:
Maximum
x"T
y"T
z"T"
Ttype:	
2	Р
К
Mean

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( "
Ttype:
2	"
Tidxtype0:
2	
b
MergeV2Checkpoints
checkpoint_prefixes
destination_prefix"
delete_old_dirsbool(
<
Mul
x"T
y"T
z"T"
Ttype:
2	Р

NoOp
M
Pack
values"T*N
output"T"
Nint(0"	
Ttype"
axisint 
я
ParseExample

serialized	
names
sparse_keys*Nsparse

dense_keys*Ndense
dense_defaults2Tdense
sparse_indices	*Nsparse
sparse_values2sparse_types
sparse_shapes	*Nsparse
dense_values2Tdense"
Nsparseint("
Ndenseint("%
sparse_types
list(type)(:
2	"
Tdense
list(type)(:
2	"
dense_shapeslist(shape)(
A
Placeholder
output"dtype"
dtypetype"
shapeshape: 
К
Prod

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( "
Ttype:
2	"
Tidxtype0:
2	
}
RandomUniform

shape"T
output"dtype"
seedint "
seed2int "
dtypetype:
2"
Ttype:
2	И
`
Range
start"Tidx
limit"Tidx
delta"Tidx
output"Tidx"
Tidxtype0:
2	
=
RealDiv
x"T
y"T
z"T"
Ttype:
2	
A
Relu
features"T
activations"T"
Ttype:
2		
S
ReluGrad
	gradients"T
features"T
	backprops"T"
Ttype:
2		
[
Reshape
tensor"T
shape"Tshape
output"T"	
Ttype"
Tshapetype0:
2	
l
	RestoreV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0
i
SaveV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0
P
Shape

input"T
output"out_type"	
Ttype"
out_typetype0:
2	
H
ShardedFilename
basename	
shard

num_shards
filename
a
Slice

input"T
begin"Index
size"Index
output"T"	
Ttype"
Indextype:
2	
i
SoftmaxCrossEntropyWithLogits
features"T
labels"T	
loss"T
backprop"T"
Ttype:
2
N

StringJoin
inputs*N

output"
Nint(0"
	separatorstring 
5
Sub
x"T
y"T
z"T"
Ttype:
	2	
Й
Sum

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( "
Ttype:
2	"
Tidxtype0:
2	
c
Tile

input"T
	multiples"
Tmultiples
output"T"	
Ttype"

Tmultiplestype0:
2	
c
TopKV2

input"T
k
values"T
indices"
sortedbool("
Ttype:
2		
s

VariableV2
ref"dtypeА"
shapeshape"
dtypetype"
	containerstring "
shared_namestring И
&
	ZerosLike
x"T
y"T"	
Ttype"serve*1.1.02v1.1.0-rc0-61-g1ec6ed5Ю∙
M

tf_examplePlaceholder*
shape: *
_output_shapes
:*
dtype0
U
ParseExample/ConstConst*
valueB *
dtype0*
_output_shapes
: 
b
ParseExample/ParseExample/namesConst*
valueB *
dtype0*
_output_shapes
: 
h
&ParseExample/ParseExample/dense_keys_0Const*
value	B Bx*
dtype0*
_output_shapes
: 
Щ
ParseExample/ParseExampleParseExample
tf_exampleParseExample/ParseExample/names&ParseExample/ParseExample/dense_keys_0ParseExample/Const*
Tdense
2*
sparse_types
 *
dense_shapes	
:Р*(
_output_shapes
:         Р*
Ndense*
Nsparse 
[
xIdentityParseExample/ParseExample*(
_output_shapes
:         Р*
T0
]
PlaceholderPlaceholder*
shape: *'
_output_shapes
:         
*
dtype0
e
random_uniform/shapeConst*
valueB"     *
dtype0*
_output_shapes
:
W
random_uniform/minConst*
valueB
 *  А┐*
dtype0*
_output_shapes
: 
W
random_uniform/maxConst*
valueB
 *  А?*
dtype0*
_output_shapes
: 
Ф
random_uniform/RandomUniformRandomUniformrandom_uniform/shape*
dtype0*

seed *
T0*
seed2 * 
_output_shapes
:
РА
b
random_uniform/subSubrandom_uniform/maxrandom_uniform/min*
_output_shapes
: *
T0
v
random_uniform/mulMulrandom_uniform/RandomUniformrandom_uniform/sub* 
_output_shapes
:
РА*
T0
h
random_uniformAddrandom_uniform/mulrandom_uniform/min* 
_output_shapes
:
РА*
T0
А
Variable
VariableV2*
	container *
shape:
РА* 
_output_shapes
:
РА*
dtype0*
shared_name 
д
Variable/AssignAssignVariablerandom_uniform*
_class
loc:@Variable*
validate_shape(*
T0* 
_output_shapes
:
РА*
use_locking(
k
Variable/readIdentityVariable*
_class
loc:@Variable* 
_output_shapes
:
РА*
T0
g
random_uniform_1/shapeConst*
valueB"      *
dtype0*
_output_shapes
:
Y
random_uniform_1/minConst*
valueB
 *  А┐*
dtype0*
_output_shapes
: 
Y
random_uniform_1/maxConst*
valueB
 *  А?*
dtype0*
_output_shapes
: 
Ш
random_uniform_1/RandomUniformRandomUniformrandom_uniform_1/shape*
dtype0*

seed *
T0*
seed2 * 
_output_shapes
:
АА
h
random_uniform_1/subSubrandom_uniform_1/maxrandom_uniform_1/min*
_output_shapes
: *
T0
|
random_uniform_1/mulMulrandom_uniform_1/RandomUniformrandom_uniform_1/sub* 
_output_shapes
:
АА*
T0
n
random_uniform_1Addrandom_uniform_1/mulrandom_uniform_1/min* 
_output_shapes
:
АА*
T0
В

Variable_1
VariableV2*
	container *
shape:
АА* 
_output_shapes
:
АА*
dtype0*
shared_name 
м
Variable_1/AssignAssign
Variable_1random_uniform_1*
_class
loc:@Variable_1*
validate_shape(*
T0* 
_output_shapes
:
АА*
use_locking(
q
Variable_1/readIdentity
Variable_1*
_class
loc:@Variable_1* 
_output_shapes
:
АА*
T0
g
random_uniform_2/shapeConst*
valueB"   
   *
dtype0*
_output_shapes
:
Y
random_uniform_2/minConst*
valueB
 *  А┐*
dtype0*
_output_shapes
: 
Y
random_uniform_2/maxConst*
valueB
 *  А?*
dtype0*
_output_shapes
: 
Ч
random_uniform_2/RandomUniformRandomUniformrandom_uniform_2/shape*
dtype0*

seed *
T0*
seed2 *
_output_shapes
:	А

h
random_uniform_2/subSubrandom_uniform_2/maxrandom_uniform_2/min*
_output_shapes
: *
T0
{
random_uniform_2/mulMulrandom_uniform_2/RandomUniformrandom_uniform_2/sub*
_output_shapes
:	А
*
T0
m
random_uniform_2Addrandom_uniform_2/mulrandom_uniform_2/min*
_output_shapes
:	А
*
T0
А

Variable_2
VariableV2*
	container *
shape:	А
*
_output_shapes
:	А
*
dtype0*
shared_name 
л
Variable_2/AssignAssign
Variable_2random_uniform_2*
_class
loc:@Variable_2*
validate_shape(*
T0*
_output_shapes
:	А
*
use_locking(
p
Variable_2/readIdentity
Variable_2*
_class
loc:@Variable_2*
_output_shapes
:	А
*
T0
a
random_uniform_3/shapeConst*
valueB:А*
dtype0*
_output_shapes
:
Y
random_uniform_3/minConst*
valueB
 *  А┐*
dtype0*
_output_shapes
: 
Y
random_uniform_3/maxConst*
valueB
 *  А?*
dtype0*
_output_shapes
: 
У
random_uniform_3/RandomUniformRandomUniformrandom_uniform_3/shape*
dtype0*

seed *
T0*
seed2 *
_output_shapes	
:А
h
random_uniform_3/subSubrandom_uniform_3/maxrandom_uniform_3/min*
_output_shapes
: *
T0
w
random_uniform_3/mulMulrandom_uniform_3/RandomUniformrandom_uniform_3/sub*
_output_shapes	
:А*
T0
i
random_uniform_3Addrandom_uniform_3/mulrandom_uniform_3/min*
_output_shapes	
:А*
T0
x

Variable_3
VariableV2*
	container *
shape:А*
_output_shapes	
:А*
dtype0*
shared_name 
з
Variable_3/AssignAssign
Variable_3random_uniform_3*
_class
loc:@Variable_3*
validate_shape(*
T0*
_output_shapes	
:А*
use_locking(
l
Variable_3/readIdentity
Variable_3*
_class
loc:@Variable_3*
_output_shapes	
:А*
T0
a
random_uniform_4/shapeConst*
valueB:А*
dtype0*
_output_shapes
:
Y
random_uniform_4/minConst*
valueB
 *  А┐*
dtype0*
_output_shapes
: 
Y
random_uniform_4/maxConst*
valueB
 *  А?*
dtype0*
_output_shapes
: 
У
random_uniform_4/RandomUniformRandomUniformrandom_uniform_4/shape*
dtype0*

seed *
T0*
seed2 *
_output_shapes	
:А
h
random_uniform_4/subSubrandom_uniform_4/maxrandom_uniform_4/min*
_output_shapes
: *
T0
w
random_uniform_4/mulMulrandom_uniform_4/RandomUniformrandom_uniform_4/sub*
_output_shapes	
:А*
T0
i
random_uniform_4Addrandom_uniform_4/mulrandom_uniform_4/min*
_output_shapes	
:А*
T0
x

Variable_4
VariableV2*
	container *
shape:А*
_output_shapes	
:А*
dtype0*
shared_name 
з
Variable_4/AssignAssign
Variable_4random_uniform_4*
_class
loc:@Variable_4*
validate_shape(*
T0*
_output_shapes	
:А*
use_locking(
l
Variable_4/readIdentity
Variable_4*
_class
loc:@Variable_4*
_output_shapes	
:А*
T0
`
random_uniform_5/shapeConst*
valueB:
*
dtype0*
_output_shapes
:
Y
random_uniform_5/minConst*
valueB
 *  А┐*
dtype0*
_output_shapes
: 
Y
random_uniform_5/maxConst*
valueB
 *  А?*
dtype0*
_output_shapes
: 
Т
random_uniform_5/RandomUniformRandomUniformrandom_uniform_5/shape*
dtype0*

seed *
T0*
seed2 *
_output_shapes
:

h
random_uniform_5/subSubrandom_uniform_5/maxrandom_uniform_5/min*
_output_shapes
: *
T0
v
random_uniform_5/mulMulrandom_uniform_5/RandomUniformrandom_uniform_5/sub*
_output_shapes
:
*
T0
h
random_uniform_5Addrandom_uniform_5/mulrandom_uniform_5/min*
_output_shapes
:
*
T0
v

Variable_5
VariableV2*
	container *
shape:
*
_output_shapes
:
*
dtype0*
shared_name 
ж
Variable_5/AssignAssign
Variable_5random_uniform_5*
_class
loc:@Variable_5*
validate_shape(*
T0*
_output_shapes
:
*
use_locking(
k
Variable_5/readIdentity
Variable_5*
_class
loc:@Variable_5*
_output_shapes
:
*
T0
{
MatMulMatMulxVariable/read*(
_output_shapes
:         А*
T0*
transpose_a( *
transpose_b( 
V
AddAddMatMulVariable_3/read*(
_output_shapes
:         А*
T0
D
ReluReluAdd*(
_output_shapes
:         А*
T0
В
MatMul_1MatMulReluVariable_1/read*(
_output_shapes
:         А*
T0*
transpose_a( *
transpose_b( 
Z
Add_1AddMatMul_1Variable_4/read*(
_output_shapes
:         А*
T0
H
Relu_1ReluAdd_1*(
_output_shapes
:         А*
T0
Г
MatMul_2MatMulRelu_1Variable_2/read*'
_output_shapes
:         
*
T0*
transpose_a( *
transpose_b( 
W
addAddMatMul_2Variable_5/read*'
_output_shapes
:         
*
T0
J
TopKV2/kConst*
value	B :
*
dtype0*
_output_shapes
: 
r
TopKV2TopKV2addTopKV2/k*
sorted(*:
_output_shapes(
&:         
:         
*
T0
Z
ToInt64CastTopKV2:1*'
_output_shapes
:         
*

SrcT0*

DstT0	
j
ConstConst*1
value(B&
B0B1B2B3B4B5B6B7B8B9*
dtype0*
_output_shapes
:

V
index_to_string/SizeConst*
value	B :
*
dtype0*
_output_shapes
: 
]
index_to_string/range/startConst*
value	B : *
dtype0*
_output_shapes
: 
]
index_to_string/range/deltaConst*
value	B :*
dtype0*
_output_shapes
: 
Ц
index_to_string/rangeRangeindex_to_string/range/startindex_to_string/Sizeindex_to_string/range/delta*

Tidx0*
_output_shapes
:

j
index_to_string/ToInt64Castindex_to_string/range*
_output_shapes
:
*

SrcT0*

DstT0	
Э
index_to_string	HashTable*
	key_dtype0	*
shared_name *
value_dtype0*
	container *
_output_shapes
:*
use_node_name_sharing( 
Y
index_to_string/ConstConst*
valueB	 BUNK*
dtype0*
_output_shapes
: 
Ъ
index_to_string/table_initInitializeTableindex_to_stringindex_to_string/ToInt64Const*"
_class
loc:@index_to_string*

Tkey0	*

Tval0
╛
index_to_string_LookupLookupTableFindindex_to_stringToInt64index_to_string/Const*"
_class
loc:@index_to_string*	
Tin0	*'
_output_shapes
:         
*

Tout0
F
RankConst*
value	B :*
dtype0*
_output_shapes
: 
H
ShapeShapeadd*
_output_shapes
:*
T0*
out_type0
H
Rank_1Const*
value	B :*
dtype0*
_output_shapes
: 
J
Shape_1Shapeadd*
_output_shapes
:*
T0*
out_type0
G
Sub/yConst*
value	B :*
dtype0*
_output_shapes
: 
:
SubSubRank_1Sub/y*
_output_shapes
: *
T0
R
Slice/beginPackSub*
_output_shapes
:*
T0*

axis *
N
T

Slice/sizeConst*
valueB:*
dtype0*
_output_shapes
:
b
SliceSliceShape_1Slice/begin
Slice/size*
T0*
_output_shapes
:*
Index0
b
concat/values_0Const*
valueB:
         *
dtype0*
_output_shapes
:
M
concat/axisConst*
value	B : *
dtype0*
_output_shapes
: 
q
concatConcatV2concat/values_0Sliceconcat/axis*

Tidx0*
_output_shapes
:*
T0*
N
h
ReshapeReshapeaddconcat*0
_output_shapes
:                  *
T0*
Tshape0
H
Rank_2Const*
value	B :*
dtype0*
_output_shapes
: 
R
Shape_2ShapePlaceholder*
_output_shapes
:*
T0*
out_type0
I
Sub_1/yConst*
value	B :*
dtype0*
_output_shapes
: 
>
Sub_1SubRank_2Sub_1/y*
_output_shapes
: *
T0
V
Slice_1/beginPackSub_1*
_output_shapes
:*
T0*

axis *
N
V
Slice_1/sizeConst*
valueB:*
dtype0*
_output_shapes
:
h
Slice_1SliceShape_2Slice_1/beginSlice_1/size*
T0*
_output_shapes
:*
Index0
d
concat_1/values_0Const*
valueB:
         *
dtype0*
_output_shapes
:
O
concat_1/axisConst*
value	B : *
dtype0*
_output_shapes
: 
y
concat_1ConcatV2concat_1/values_0Slice_1concat_1/axis*

Tidx0*
_output_shapes
:*
T0*
N
t
	Reshape_1ReshapePlaceholderconcat_1*0
_output_shapes
:                  *
T0*
Tshape0
Ь
SoftmaxCrossEntropyWithLogitsSoftmaxCrossEntropyWithLogitsReshape	Reshape_1*?
_output_shapes-
+:         :                  *
T0
I
Sub_2/yConst*
value	B :*
dtype0*
_output_shapes
: 
<
Sub_2SubRankSub_2/y*
_output_shapes
: *
T0
W
Slice_2/beginConst*
valueB: *
dtype0*
_output_shapes
:
U
Slice_2/sizePackSub_2*
_output_shapes
:*
T0*

axis *
N
o
Slice_2SliceShapeSlice_2/beginSlice_2/size*
T0*#
_output_shapes
:         *
Index0
x
	Reshape_2ReshapeSoftmaxCrossEntropyWithLogitsSlice_2*#
_output_shapes
:         *
T0*
Tshape0
Q
Const_1Const*
valueB: *
dtype0*
_output_shapes
:
^
MeanMean	Reshape_2Const_1*

Tidx0*
_output_shapes
: *
T0*
	keep_dims( 
R
gradients/ShapeConst*
valueB *
dtype0*
_output_shapes
: 
T
gradients/ConstConst*
valueB
 *  А?*
dtype0*
_output_shapes
: 
Y
gradients/FillFillgradients/Shapegradients/Const*
_output_shapes
: *
T0
k
!gradients/Mean_grad/Reshape/shapeConst*
valueB:*
dtype0*
_output_shapes
:
М
gradients/Mean_grad/ReshapeReshapegradients/Fill!gradients/Mean_grad/Reshape/shape*
_output_shapes
:*
T0*
Tshape0
b
gradients/Mean_grad/ShapeShape	Reshape_2*
_output_shapes
:*
T0*
out_type0
Ш
gradients/Mean_grad/TileTilegradients/Mean_grad/Reshapegradients/Mean_grad/Shape*

Tmultiples0*#
_output_shapes
:         *
T0
d
gradients/Mean_grad/Shape_1Shape	Reshape_2*
_output_shapes
:*
T0*
out_type0
^
gradients/Mean_grad/Shape_2Const*
valueB *
dtype0*
_output_shapes
: 
c
gradients/Mean_grad/ConstConst*
valueB: *
dtype0*
_output_shapes
:
Ц
gradients/Mean_grad/ProdProdgradients/Mean_grad/Shape_1gradients/Mean_grad/Const*

Tidx0*
_output_shapes
: *
T0*
	keep_dims( 
e
gradients/Mean_grad/Const_1Const*
valueB: *
dtype0*
_output_shapes
:
Ъ
gradients/Mean_grad/Prod_1Prodgradients/Mean_grad/Shape_2gradients/Mean_grad/Const_1*

Tidx0*
_output_shapes
: *
T0*
	keep_dims( 
_
gradients/Mean_grad/Maximum/yConst*
value	B :*
dtype0*
_output_shapes
: 
В
gradients/Mean_grad/MaximumMaximumgradients/Mean_grad/Prod_1gradients/Mean_grad/Maximum/y*
_output_shapes
: *
T0
А
gradients/Mean_grad/floordivFloorDivgradients/Mean_grad/Prodgradients/Mean_grad/Maximum*
_output_shapes
: *
T0
n
gradients/Mean_grad/CastCastgradients/Mean_grad/floordiv*
_output_shapes
: *

SrcT0*

DstT0
И
gradients/Mean_grad/truedivRealDivgradients/Mean_grad/Tilegradients/Mean_grad/Cast*#
_output_shapes
:         *
T0
{
gradients/Reshape_2_grad/ShapeShapeSoftmaxCrossEntropyWithLogits*
_output_shapes
:*
T0*
out_type0
д
 gradients/Reshape_2_grad/ReshapeReshapegradients/Mean_grad/truedivgradients/Reshape_2_grad/Shape*#
_output_shapes
:         *
T0*
Tshape0
}
gradients/zeros_like	ZerosLikeSoftmaxCrossEntropyWithLogits:1*0
_output_shapes
:                  *
T0
Ж
;gradients/SoftmaxCrossEntropyWithLogits_grad/ExpandDims/dimConst*
valueB :
         *
dtype0*
_output_shapes
: 
т
7gradients/SoftmaxCrossEntropyWithLogits_grad/ExpandDims
ExpandDims gradients/Reshape_2_grad/Reshape;gradients/SoftmaxCrossEntropyWithLogits_grad/ExpandDims/dim*'
_output_shapes
:         *
T0*

Tdim0
╠
0gradients/SoftmaxCrossEntropyWithLogits_grad/mulMul7gradients/SoftmaxCrossEntropyWithLogits_grad/ExpandDimsSoftmaxCrossEntropyWithLogits:1*0
_output_shapes
:                  *
T0
_
gradients/Reshape_grad/ShapeShapeadd*
_output_shapes
:*
T0*
out_type0
╣
gradients/Reshape_grad/ReshapeReshape0gradients/SoftmaxCrossEntropyWithLogits_grad/mulgradients/Reshape_grad/Shape*'
_output_shapes
:         
*
T0*
Tshape0
`
gradients/add_grad/ShapeShapeMatMul_2*
_output_shapes
:*
T0*
out_type0
d
gradients/add_grad/Shape_1Const*
valueB:
*
dtype0*
_output_shapes
:
┤
(gradients/add_grad/BroadcastGradientArgsBroadcastGradientArgsgradients/add_grad/Shapegradients/add_grad/Shape_1*2
_output_shapes 
:         :         *
T0
з
gradients/add_grad/SumSumgradients/Reshape_grad/Reshape(gradients/add_grad/BroadcastGradientArgs*

Tidx0*
_output_shapes
:*
T0*
	keep_dims( 
Ч
gradients/add_grad/ReshapeReshapegradients/add_grad/Sumgradients/add_grad/Shape*'
_output_shapes
:         
*
T0*
Tshape0
л
gradients/add_grad/Sum_1Sumgradients/Reshape_grad/Reshape*gradients/add_grad/BroadcastGradientArgs:1*

Tidx0*
_output_shapes
:*
T0*
	keep_dims( 
Р
gradients/add_grad/Reshape_1Reshapegradients/add_grad/Sum_1gradients/add_grad/Shape_1*
_output_shapes
:
*
T0*
Tshape0
g
#gradients/add_grad/tuple/group_depsNoOp^gradients/add_grad/Reshape^gradients/add_grad/Reshape_1
┌
+gradients/add_grad/tuple/control_dependencyIdentitygradients/add_grad/Reshape$^gradients/add_grad/tuple/group_deps*-
_class#
!loc:@gradients/add_grad/Reshape*'
_output_shapes
:         
*
T0
╙
-gradients/add_grad/tuple/control_dependency_1Identitygradients/add_grad/Reshape_1$^gradients/add_grad/tuple/group_deps*/
_class%
#!loc:@gradients/add_grad/Reshape_1*
_output_shapes
:
*
T0
┐
gradients/MatMul_2_grad/MatMulMatMul+gradients/add_grad/tuple/control_dependencyVariable_2/read*(
_output_shapes
:         А*
T0*
transpose_a( *
transpose_b(
п
 gradients/MatMul_2_grad/MatMul_1MatMulRelu_1+gradients/add_grad/tuple/control_dependency*
_output_shapes
:	А
*
T0*
transpose_a(*
transpose_b( 
t
(gradients/MatMul_2_grad/tuple/group_depsNoOp^gradients/MatMul_2_grad/MatMul!^gradients/MatMul_2_grad/MatMul_1
э
0gradients/MatMul_2_grad/tuple/control_dependencyIdentitygradients/MatMul_2_grad/MatMul)^gradients/MatMul_2_grad/tuple/group_deps*1
_class'
%#loc:@gradients/MatMul_2_grad/MatMul*(
_output_shapes
:         А*
T0
ъ
2gradients/MatMul_2_grad/tuple/control_dependency_1Identity gradients/MatMul_2_grad/MatMul_1)^gradients/MatMul_2_grad/tuple/group_deps*3
_class)
'%loc:@gradients/MatMul_2_grad/MatMul_1*
_output_shapes
:	А
*
T0
Ч
gradients/Relu_1_grad/ReluGradReluGrad0gradients/MatMul_2_grad/tuple/control_dependencyRelu_1*(
_output_shapes
:         А*
T0
b
gradients/Add_1_grad/ShapeShapeMatMul_1*
_output_shapes
:*
T0*
out_type0
g
gradients/Add_1_grad/Shape_1Const*
valueB:А*
dtype0*
_output_shapes
:
║
*gradients/Add_1_grad/BroadcastGradientArgsBroadcastGradientArgsgradients/Add_1_grad/Shapegradients/Add_1_grad/Shape_1*2
_output_shapes 
:         :         *
T0
л
gradients/Add_1_grad/SumSumgradients/Relu_1_grad/ReluGrad*gradients/Add_1_grad/BroadcastGradientArgs*

Tidx0*
_output_shapes
:*
T0*
	keep_dims( 
Ю
gradients/Add_1_grad/ReshapeReshapegradients/Add_1_grad/Sumgradients/Add_1_grad/Shape*(
_output_shapes
:         А*
T0*
Tshape0
п
gradients/Add_1_grad/Sum_1Sumgradients/Relu_1_grad/ReluGrad,gradients/Add_1_grad/BroadcastGradientArgs:1*

Tidx0*
_output_shapes
:*
T0*
	keep_dims( 
Ч
gradients/Add_1_grad/Reshape_1Reshapegradients/Add_1_grad/Sum_1gradients/Add_1_grad/Shape_1*
_output_shapes	
:А*
T0*
Tshape0
m
%gradients/Add_1_grad/tuple/group_depsNoOp^gradients/Add_1_grad/Reshape^gradients/Add_1_grad/Reshape_1
у
-gradients/Add_1_grad/tuple/control_dependencyIdentitygradients/Add_1_grad/Reshape&^gradients/Add_1_grad/tuple/group_deps*/
_class%
#!loc:@gradients/Add_1_grad/Reshape*(
_output_shapes
:         А*
T0
▄
/gradients/Add_1_grad/tuple/control_dependency_1Identitygradients/Add_1_grad/Reshape_1&^gradients/Add_1_grad/tuple/group_deps*1
_class'
%#loc:@gradients/Add_1_grad/Reshape_1*
_output_shapes	
:А*
T0
┴
gradients/MatMul_1_grad/MatMulMatMul-gradients/Add_1_grad/tuple/control_dependencyVariable_1/read*(
_output_shapes
:         А*
T0*
transpose_a( *
transpose_b(
░
 gradients/MatMul_1_grad/MatMul_1MatMulRelu-gradients/Add_1_grad/tuple/control_dependency* 
_output_shapes
:
АА*
T0*
transpose_a(*
transpose_b( 
t
(gradients/MatMul_1_grad/tuple/group_depsNoOp^gradients/MatMul_1_grad/MatMul!^gradients/MatMul_1_grad/MatMul_1
э
0gradients/MatMul_1_grad/tuple/control_dependencyIdentitygradients/MatMul_1_grad/MatMul)^gradients/MatMul_1_grad/tuple/group_deps*1
_class'
%#loc:@gradients/MatMul_1_grad/MatMul*(
_output_shapes
:         А*
T0
ы
2gradients/MatMul_1_grad/tuple/control_dependency_1Identity gradients/MatMul_1_grad/MatMul_1)^gradients/MatMul_1_grad/tuple/group_deps*3
_class)
'%loc:@gradients/MatMul_1_grad/MatMul_1* 
_output_shapes
:
АА*
T0
У
gradients/Relu_grad/ReluGradReluGrad0gradients/MatMul_1_grad/tuple/control_dependencyRelu*(
_output_shapes
:         А*
T0
^
gradients/Add_grad/ShapeShapeMatMul*
_output_shapes
:*
T0*
out_type0
e
gradients/Add_grad/Shape_1Const*
valueB:А*
dtype0*
_output_shapes
:
┤
(gradients/Add_grad/BroadcastGradientArgsBroadcastGradientArgsgradients/Add_grad/Shapegradients/Add_grad/Shape_1*2
_output_shapes 
:         :         *
T0
е
gradients/Add_grad/SumSumgradients/Relu_grad/ReluGrad(gradients/Add_grad/BroadcastGradientArgs*

Tidx0*
_output_shapes
:*
T0*
	keep_dims( 
Ш
gradients/Add_grad/ReshapeReshapegradients/Add_grad/Sumgradients/Add_grad/Shape*(
_output_shapes
:         А*
T0*
Tshape0
й
gradients/Add_grad/Sum_1Sumgradients/Relu_grad/ReluGrad*gradients/Add_grad/BroadcastGradientArgs:1*

Tidx0*
_output_shapes
:*
T0*
	keep_dims( 
С
gradients/Add_grad/Reshape_1Reshapegradients/Add_grad/Sum_1gradients/Add_grad/Shape_1*
_output_shapes	
:А*
T0*
Tshape0
g
#gradients/Add_grad/tuple/group_depsNoOp^gradients/Add_grad/Reshape^gradients/Add_grad/Reshape_1
█
+gradients/Add_grad/tuple/control_dependencyIdentitygradients/Add_grad/Reshape$^gradients/Add_grad/tuple/group_deps*-
_class#
!loc:@gradients/Add_grad/Reshape*(
_output_shapes
:         А*
T0
╘
-gradients/Add_grad/tuple/control_dependency_1Identitygradients/Add_grad/Reshape_1$^gradients/Add_grad/tuple/group_deps*/
_class%
#!loc:@gradients/Add_grad/Reshape_1*
_output_shapes	
:А*
T0
╗
gradients/MatMul_grad/MatMulMatMul+gradients/Add_grad/tuple/control_dependencyVariable/read*(
_output_shapes
:         Р*
T0*
transpose_a( *
transpose_b(
й
gradients/MatMul_grad/MatMul_1MatMulx+gradients/Add_grad/tuple/control_dependency* 
_output_shapes
:
РА*
T0*
transpose_a(*
transpose_b( 
n
&gradients/MatMul_grad/tuple/group_depsNoOp^gradients/MatMul_grad/MatMul^gradients/MatMul_grad/MatMul_1
х
.gradients/MatMul_grad/tuple/control_dependencyIdentitygradients/MatMul_grad/MatMul'^gradients/MatMul_grad/tuple/group_deps*/
_class%
#!loc:@gradients/MatMul_grad/MatMul*(
_output_shapes
:         Р*
T0
у
0gradients/MatMul_grad/tuple/control_dependency_1Identitygradients/MatMul_grad/MatMul_1'^gradients/MatMul_grad/tuple/group_deps*1
_class'
%#loc:@gradients/MatMul_grad/MatMul_1* 
_output_shapes
:
РА*
T0
b
GradientDescent/learning_rateConst*
valueB
 *╖╤8*
dtype0*
_output_shapes
: 
В
4GradientDescent/update_Variable/ApplyGradientDescentApplyGradientDescentVariableGradientDescent/learning_rate0gradients/MatMul_grad/tuple/control_dependency_1*
_class
loc:@Variable* 
_output_shapes
:
РА*
T0*
use_locking( 
К
6GradientDescent/update_Variable_1/ApplyGradientDescentApplyGradientDescent
Variable_1GradientDescent/learning_rate2gradients/MatMul_1_grad/tuple/control_dependency_1*
_class
loc:@Variable_1* 
_output_shapes
:
АА*
T0*
use_locking( 
Й
6GradientDescent/update_Variable_2/ApplyGradientDescentApplyGradientDescent
Variable_2GradientDescent/learning_rate2gradients/MatMul_2_grad/tuple/control_dependency_1*
_class
loc:@Variable_2*
_output_shapes
:	А
*
T0*
use_locking( 
А
6GradientDescent/update_Variable_3/ApplyGradientDescentApplyGradientDescent
Variable_3GradientDescent/learning_rate-gradients/Add_grad/tuple/control_dependency_1*
_class
loc:@Variable_3*
_output_shapes	
:А*
T0*
use_locking( 
В
6GradientDescent/update_Variable_4/ApplyGradientDescentApplyGradientDescent
Variable_4GradientDescent/learning_rate/gradients/Add_1_grad/tuple/control_dependency_1*
_class
loc:@Variable_4*
_output_shapes	
:А*
T0*
use_locking( 
 
6GradientDescent/update_Variable_5/ApplyGradientDescentApplyGradientDescent
Variable_5GradientDescent/learning_rate-gradients/add_grad/tuple/control_dependency_1*
_class
loc:@Variable_5*
_output_shapes
:
*
T0*
use_locking( 
ы
GradientDescentNoOp5^GradientDescent/update_Variable/ApplyGradientDescent7^GradientDescent/update_Variable_1/ApplyGradientDescent7^GradientDescent/update_Variable_2/ApplyGradientDescent7^GradientDescent/update_Variable_3/ApplyGradientDescent7^GradientDescent/update_Variable_4/ApplyGradientDescent7^GradientDescent/update_Variable_5/ApplyGradientDescent
В
initNoOp^Variable/Assign^Variable_1/Assign^Variable_2/Assign^Variable_3/Assign^Variable_4/Assign^Variable_5/Assign
4
init_all_tablesNoOp^index_to_string/table_init
(
legacy_init_opNoOp^init_all_tables
P

save/ConstConst*
valueB Bmodel*
dtype0*
_output_shapes
: 
Д
save/StringJoin/inputs_1Const*<
value3B1 B+_temp_ed086aa955864e2a85e7c43862c52150/part*
dtype0*
_output_shapes
: 
u
save/StringJoin
StringJoin
save/Constsave/StringJoin/inputs_1*
	separator *
_output_shapes
: *
N
Q
save/num_shardsConst*
value	B :*
dtype0*
_output_shapes
: 
\
save/ShardedFilename/shardConst*
value	B : *
dtype0*
_output_shapes
: 
}
save/ShardedFilenameShardedFilenamesave/StringJoinsave/ShardedFilename/shardsave/num_shards*
_output_shapes
: 
е
save/SaveV2/tensor_namesConst*Y
valuePBNBVariableB
Variable_1B
Variable_2B
Variable_3B
Variable_4B
Variable_5*
dtype0*
_output_shapes
:
o
save/SaveV2/shape_and_slicesConst*
valueBB B B B B B *
dtype0*
_output_shapes
:
┐
save/SaveV2SaveV2save/ShardedFilenamesave/SaveV2/tensor_namessave/SaveV2/shape_and_slicesVariable
Variable_1
Variable_2
Variable_3
Variable_4
Variable_5*
dtypes

2
С
save/control_dependencyIdentitysave/ShardedFilename^save/SaveV2*'
_class
loc:@save/ShardedFilename*
_output_shapes
: *
T0
Э
+save/MergeV2Checkpoints/checkpoint_prefixesPacksave/ShardedFilename^save/control_dependency*
_output_shapes
:*
T0*

axis *
N
}
save/MergeV2CheckpointsMergeV2Checkpoints+save/MergeV2Checkpoints/checkpoint_prefixes
save/Const*
delete_old_dirs(
z
save/IdentityIdentity
save/Const^save/control_dependency^save/MergeV2Checkpoints*
_output_shapes
: *
T0
l
save/RestoreV2/tensor_namesConst*
valueBBVariable*
dtype0*
_output_shapes
:
h
save/RestoreV2/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
Р
save/RestoreV2	RestoreV2
save/Constsave/RestoreV2/tensor_namessave/RestoreV2/shape_and_slices*
_output_shapes
:*
dtypes
2
а
save/AssignAssignVariablesave/RestoreV2*
_class
loc:@Variable*
validate_shape(*
T0* 
_output_shapes
:
РА*
use_locking(
p
save/RestoreV2_1/tensor_namesConst*
valueBB
Variable_1*
dtype0*
_output_shapes
:
j
!save/RestoreV2_1/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
Ц
save/RestoreV2_1	RestoreV2
save/Constsave/RestoreV2_1/tensor_names!save/RestoreV2_1/shape_and_slices*
_output_shapes
:*
dtypes
2
и
save/Assign_1Assign
Variable_1save/RestoreV2_1*
_class
loc:@Variable_1*
validate_shape(*
T0* 
_output_shapes
:
АА*
use_locking(
p
save/RestoreV2_2/tensor_namesConst*
valueBB
Variable_2*
dtype0*
_output_shapes
:
j
!save/RestoreV2_2/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
Ц
save/RestoreV2_2	RestoreV2
save/Constsave/RestoreV2_2/tensor_names!save/RestoreV2_2/shape_and_slices*
_output_shapes
:*
dtypes
2
з
save/Assign_2Assign
Variable_2save/RestoreV2_2*
_class
loc:@Variable_2*
validate_shape(*
T0*
_output_shapes
:	А
*
use_locking(
p
save/RestoreV2_3/tensor_namesConst*
valueBB
Variable_3*
dtype0*
_output_shapes
:
j
!save/RestoreV2_3/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
Ц
save/RestoreV2_3	RestoreV2
save/Constsave/RestoreV2_3/tensor_names!save/RestoreV2_3/shape_and_slices*
_output_shapes
:*
dtypes
2
г
save/Assign_3Assign
Variable_3save/RestoreV2_3*
_class
loc:@Variable_3*
validate_shape(*
T0*
_output_shapes	
:А*
use_locking(
p
save/RestoreV2_4/tensor_namesConst*
valueBB
Variable_4*
dtype0*
_output_shapes
:
j
!save/RestoreV2_4/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
Ц
save/RestoreV2_4	RestoreV2
save/Constsave/RestoreV2_4/tensor_names!save/RestoreV2_4/shape_and_slices*
_output_shapes
:*
dtypes
2
г
save/Assign_4Assign
Variable_4save/RestoreV2_4*
_class
loc:@Variable_4*
validate_shape(*
T0*
_output_shapes	
:А*
use_locking(
p
save/RestoreV2_5/tensor_namesConst*
valueBB
Variable_5*
dtype0*
_output_shapes
:
j
!save/RestoreV2_5/shape_and_slicesConst*
valueB
B *
dtype0*
_output_shapes
:
Ц
save/RestoreV2_5	RestoreV2
save/Constsave/RestoreV2_5/tensor_names!save/RestoreV2_5/shape_and_slices*
_output_shapes
:*
dtypes
2
в
save/Assign_5Assign
Variable_5save/RestoreV2_5*
_class
loc:@Variable_5*
validate_shape(*
T0*
_output_shapes
:
*
use_locking(
x
save/restore_shardNoOp^save/Assign^save/Assign_1^save/Assign_2^save/Assign_3^save/Assign_4^save/Assign_5
-
save/restore_allNoOp^save/restore_shard"<
save/Const:0save/Identity:0save/restore_all (5 @F8"$
legacy_init_op

legacy_init_op"3
table_initializer

index_to_string/table_init"
train_op

GradientDescent"┘
trainable_variables┴╛
.

Variable:0Variable/AssignVariable/read:0
4
Variable_1:0Variable_1/AssignVariable_1/read:0
4
Variable_2:0Variable_2/AssignVariable_2/read:0
4
Variable_3:0Variable_3/AssignVariable_3/read:0
4
Variable_4:0Variable_4/AssignVariable_4/read:0
4
Variable_5:0Variable_5/AssignVariable_5/read:0"╧
	variables┴╛
.

Variable:0Variable/AssignVariable/read:0
4
Variable_1:0Variable_1/AssignVariable_1/read:0
4
Variable_2:0Variable_2/AssignVariable_2/read:0
4
Variable_3:0Variable_3/AssignVariable_3/read:0
4
Variable_4:0Variable_4/AssignVariable_4/read:0
4
Variable_5:0Variable_5/AssignVariable_5/read:0*~
predict_imagesl
%
inputs
x:0         Р'
outputs
add:0         
tensorflow/serving/predict*{
serving_defaulth

inputs
tf_example:0)
scores
TopKV2:0         
tensorflow/serving/classify